"""HTTP error handling that does not leak patient data.

The default FastAPI 422 body is built from ``exc.errors()``, which includes the
**rejected input value** for every failed field. For an endpoint that accepts
DICOM metadata that means a mistyped date of birth is echoed back in the
response body, written to the access log, and forwarded to whatever error
tracker sits in front of the service. A validation bug becomes a personal-data
disclosure — the kind of incident UK GDPR Art. 33 expects to be reported, and a
particularly annoying one because nobody wrote a line of code to cause it.

Every handler here therefore returns:

* a stable ``error`` code, safe to branch on from a client;
* PHI-free field-level detail (path, constraint type, constraint message);
* a ``correlation_id`` matching the ``X-Request-ID`` response header, so an
  operator can find the full context in the logs without it being in the
  response.

Rejections are also written to the audit trail. A repeated stream of invalid
submissions from one source is a data-quality signal worth seeing, and the
Art. 5(2) accountability principle expects processing decisions — including
refusals — to be demonstrable.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from common.errors import PermanentError, PipelineError, TransientError
from common.schemas import safe_validation_errors
from worker.logger import audit_logger

__all__ = ["register_exception_handlers", "correlation_id_of", "CORRELATION_HEADER"]

logger = logging.getLogger(__name__)

CORRELATION_HEADER = "X-Request-ID"


def correlation_id_of(request: Request) -> str:
    """Return this request's correlation id, generating one if absent."""
    existing = getattr(request.state, "correlation_id", None)
    if existing:
        return str(existing)
    generated = request.headers.get(CORRELATION_HEADER) or uuid.uuid4().hex
    request.state.correlation_id = generated
    return generated


def _json_error(
    request: Request,
    *,
    status_code: int,
    error: str,
    message: str,
    extra: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    correlation_id = correlation_id_of(request)
    body: dict[str, Any] = {
        "error": error,
        "message": message,
        "correlation_id": correlation_id,
    }
    if extra:
        body.update(extra)

    response_headers = {CORRELATION_HEADER: correlation_id}
    if headers:
        response_headers.update(headers)

    # FastAPI clients (and the existing tests) expect a `detail` key on error
    # responses; keep it as an alias so the contract does not break.
    body.setdefault("detail", message)
    return JSONResponse(status_code=status_code, content=body, headers=response_headers)


def register_exception_handlers(app: FastAPI) -> None:
    """Install every handler on ``app``."""

    @app.exception_handler(RequestValidationError)
    async def handle_request_validation(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        """422 with the rejected values stripped out."""
        # RequestValidationError wraps a pydantic ValidationError but is not
        # one, so it cannot be passed to safe_validation_errors directly.
        field_errors = safe_validation_errors(_as_validation_error(exc))
        correlation_id = correlation_id_of(request)

        logger.warning(
            "request_validation_failed path=%s correlation_id=%s fields=%s",
            request.url.path,
            correlation_id,
            [e["field"] for e in field_errors],
        )
        audit_logger.log_data_access(
            event_id=correlation_id,
            pseudonym_id="PS_unresolved",
            accessor=f"api:{request.url.path}",
            access_type="REJECT",
            purpose="schema_validation",
            result="rejected_invalid_payload",
        )

        return _json_error(
            request,
            status_code=422,
            error="validation_failed",
            message="Payload failed schema validation.",
            extra={"errors": field_errors},
        )

    @app.exception_handler(ValidationError)
    async def handle_model_validation(request: Request, exc: ValidationError) -> JSONResponse:
        """A model failed to validate outside request parsing.

        Almost always our bug, not the caller's — hence 500 — but the error
        still has to be rendered PHI-free before it goes anywhere.
        """
        field_errors = safe_validation_errors(exc)
        logger.error(
            "internal_validation_error path=%s fields=%s",
            request.url.path,
            [e["field"] for e in field_errors],
        )
        return _json_error(
            request,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error="internal_validation_error",
            message="The service produced data that failed its own contract.",
        )

    @app.exception_handler(PermanentError)
    async def handle_permanent(request: Request, exc: PermanentError) -> JSONResponse:
        """Caller-side error that no retry can fix -> 4xx."""
        status_code = _PERMANENT_STATUS.get(exc.code, status.HTTP_400_BAD_REQUEST)
        logger.warning(
            "request_rejected path=%s code=%s status=%d",
            request.url.path,
            exc.code,
            status_code,
        )
        return _json_error(
            request,
            status_code=status_code,
            error=exc.code,
            message=exc.message,
            extra={"errors": getattr(exc, "field_errors", [])} if getattr(exc, "field_errors", None) else None,
        )

    @app.exception_handler(TransientError)
    async def handle_transient(request: Request, exc: TransientError) -> JSONResponse:
        """Infrastructure is unhappy -> 503 with Retry-After.

        This replaces the previous behaviour of catching the exception and
        returning ``{"status": "error"}`` with HTTP **200**. A PACS reading
        only the status code would have recorded that study as successfully
        ingested when it was never published: silent clinical data loss, and
        invisible to every uptime check.
        """
        logger.error(
            "dependency_unavailable path=%s code=%s context=%s",
            request.url.path,
            exc.code,
            exc.context,
        )
        return _json_error(
            request,
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            error=exc.code,
            message="A required downstream service is unavailable. Retry this request.",
            headers={"Retry-After": "5"},
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_exception(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        return _json_error(
            request,
            status_code=exc.status_code,
            error=f"http_{exc.status_code}",
            message=str(exc.detail),
            headers=dict(getattr(exc, "headers", None) or {}),
        )

    @app.exception_handler(Exception)
    async def handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
        """Catch-all. The traceback goes to the log, never to the client.

        An unhandled exception in this service has patient data in its local
        variables by construction, so any framework default that renders a
        traceback into the response is unsafe here.
        """
        correlation_id = correlation_id_of(request)
        logger.exception(
            "unhandled_exception path=%s correlation_id=%s error=%s",
            request.url.path,
            correlation_id,
            type(exc).__name__,
        )
        return _json_error(
            request,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error="internal_error",
            message="An internal error occurred. Quote the correlation id when reporting it.",
        )


#: Permanent error codes that deserve something more specific than 400.
#: Numeric literals rather than ``status.*`` constants: Starlette renamed
#: 413/422 and deprecated the old spellings, so the codes are stable while the
#: constant names are not.
_PERMANENT_STATUS: dict[str, int] = {
    "consent_missing": 403,
    "purpose_limitation_violated": 403,
    "payload_too_large": 413,
    "schema_validation_failed": 422,
    "message_decode_failed": 400,
}


def _as_validation_error(exc: RequestValidationError) -> Any:
    """Adapt a ``RequestValidationError`` to the ``.errors()`` interface.

    ``safe_validation_errors`` calls ``errors(include_input=False, ...)``;
    ``RequestValidationError.errors()`` takes no keyword arguments, so it gets
    a thin shim rather than a second, divergent scrubbing implementation.
    """

    class _Adapter:
        @staticmethod
        def errors(**_kwargs: Any) -> list[dict[str, Any]]:
            scrubbed = []
            for error in exc.errors():
                item = {k: v for k, v in error.items() if k not in {"input", "ctx", "url"}}
                # `input` is dropped from the output, but kept here so
                # scrub_message can strip any copy a custom validator baked
                # into `msg`.
                item["input"] = error.get("input")
                item["loc"] = _strip_location_prefix(error.get("loc", ()))
                scrubbed.append(item)
            return scrubbed

    return _Adapter()


def _strip_location_prefix(loc: Any) -> tuple[Any, ...]:
    """Drop FastAPI's request-part prefix from an error location.

    FastAPI reports ``("body", "modality")``. Clients care about the field name
    in the document they sent, so the prefix is noise that also makes the error
    shape differ from the worker's (which validates the same model without a
    request around it).
    """
    parts = tuple(loc or ())
    if parts and parts[0] in {"body", "query", "path", "header", "cookie"}:
        return parts[1:] or parts
    return parts
