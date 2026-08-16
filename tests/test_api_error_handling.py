"""HTTP boundary behaviour: status codes, PHI-free bodies, health probes.

The most important assertion in this file is
``test_validation_error_body_contains_no_patient_data``. Everything else is
routine API testing; that one covers a disclosure that happens by default if
nobody intervenes.
"""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

import api.main as api_main
import api.pubsub_client as pubsub_client
from common.errors import PublishError


@pytest.fixture
def client(monkeypatch):
    """TestClient with publishing stubbed out.

    ``raise_server_exceptions=False`` so the registered 500 handler is
    exercised instead of the exception escaping into the test.
    """
    published: list[dict] = []

    def fake_publish(event, **_kwargs):
        published.append(event)
        return "msg-test-1"

    monkeypatch.setattr(api_main, "publish_event", fake_publish)
    with TestClient(api_main.app, raise_server_exceptions=False) as test_client:
        test_client.published = published  # type: ignore[attr-defined]
        yield test_client


class TestValidationResponses:
    def test_validation_error_body_contains_no_patient_data(self, client, dicom_payload):
        """FastAPI's default 422 echoes the rejected input. Ours must not."""
        dicom_payload["patient_name"] = "Jane Distinctive Doe"
        dicom_payload["patient_birth_date"] = "1985-99-99"  # invalid -> rejected

        response = client.post("/dicom/events", json=dicom_payload)
        body = response.text

        assert response.status_code == 422
        assert "Jane Distinctive Doe" not in body
        assert "1985-99-99" not in body

    def test_validation_error_is_still_actionable(self, client, dicom_payload):
        """Scrubbing must not leave the caller unable to fix their payload."""
        dicom_payload["modality"] = "PET"

        payload = client.post("/dicom/events", json=dicom_payload).json()

        assert payload["error"] == "validation_failed"
        fields = {e["field"] for e in payload["errors"]}
        assert "modality" in fields

    def test_error_entries_expose_only_safe_keys(self, client, dicom_payload):
        dicom_payload["kVp"] = -5

        payload = client.post("/dicom/events", json=dicom_payload).json()

        for entry in payload["errors"]:
            assert set(entry) == {"field", "type", "message"}

    def test_malformed_json_gets_400_without_echoing_the_body(self, client):
        response = client.post(
            "/dicom/events",
            content=b'{"patient_name": "Jane Doe", "consent_logged": tru',
            headers={"content-type": "application/json"},
        )

        assert response.status_code == 400
        assert "Jane Doe" not in response.text

    def test_oversized_body_is_rejected(self, client, dicom_payload):
        dicom_payload["source"] = "P" * (api_main.MAX_BODY_BYTES + 10)

        response = client.post("/dicom/events", json=dicom_payload)

        assert response.status_code == 413


class TestLawfulBasisGate:
    def test_missing_consent_is_403(self, client, dicom_payload):
        dicom_payload["consent_logged"] = False

        response = client.post("/dicom/events", json=dicom_payload)

        assert response.status_code == 403
        assert response.json()["error"] == "consent_missing"

    def test_absent_consent_field_is_403(self, client, dicom_payload):
        dicom_payload.pop("consent_logged")

        assert client.post("/dicom/events", json=dicom_payload).status_code == 403

    @pytest.mark.parametrize("purpose", ["research", "marketing"])
    def test_unapproved_purpose_is_403(self, client, dicom_payload, purpose):
        dicom_payload["purpose"] = purpose

        response = client.post("/dicom/events", json=dicom_payload)

        assert response.status_code == 403
        assert response.json()["error"] == "purpose_limitation_violated"

    def test_approved_purpose_is_accepted(self, client, dicom_payload):
        response = client.post("/dicom/events", json=dicom_payload)

        assert response.status_code == 200
        assert response.json()["deidentified"]["patient_name"] == "REDACTED"


class TestPublishFailureSemantics:
    def test_publish_failure_returns_503_not_200(self, monkeypatch, dicom_payload):
        """Regression for silent clinical data loss.

        The previous implementation caught the publish failure and returned
        ``{"status": "error"}`` with HTTP 200. A PACS reading the status code
        would mark the study as delivered when it had never reached the topic.
        """

        def failing_publish(_event, **_kwargs):
            raise PublishError("topic unreachable")

        monkeypatch.setattr(api_main, "publish_event", failing_publish)

        with TestClient(api_main.app, raise_server_exceptions=False) as client:
            response = client.post("/dicom/events", json=dicom_payload)

        assert response.status_code == 503
        assert response.headers["Retry-After"] == "5"
        assert response.json()["error"] == "publish_failed"

    def test_events_endpoint_also_fails_loudly(self, monkeypatch):
        def failing_publish(_event, **_kwargs):
            raise PublishError("topic unreachable")

        monkeypatch.setattr(api_main, "publish_event", failing_publish)

        with TestClient(api_main.app, raise_server_exceptions=False) as client:
            response = client.post(
                "/events",
                json={
                    "patient_id": "P123",
                    "modality": "CT",
                    "study_date": "2026-01-01",
                    "slice_thickness": 1.2,
                    "device_id": "CT_001",
                },
            )

        assert response.status_code == 503

    def test_successful_publish_returns_202_with_ids(self, client):
        response = client.post(
            "/events",
            json={
                "patient_id": "P123",
                "modality": "CT",
                "study_date": "2026-01-01",
                "slice_thickness": 1.2,
                "device_id": "CT_001",
            },
        )

        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "published"
        assert body["message_id"] == "msg-test-1"
        assert body["event_id"]


class TestCorrelationId:
    def test_response_carries_a_correlation_id(self, client, dicom_payload):
        response = client.post("/dicom/events", json=dicom_payload)

        assert response.headers["X-Request-ID"]
        assert response.json()["correlation_id"] == response.headers["X-Request-ID"]

    def test_supplied_correlation_id_is_preserved(self, client, dicom_payload):
        """Lets a caller trace one study across the API, the topic and the worker."""
        response = client.post(
            "/dicom/events", json=dicom_payload, headers={"X-Request-ID": "trace-abc"}
        )

        assert response.headers["X-Request-ID"] == "trace-abc"

    def test_error_responses_carry_the_correlation_id(self, client, dicom_payload):
        dicom_payload["modality"] = "PET"

        response = client.post("/dicom/events", json=dicom_payload)

        assert response.json()["correlation_id"] == response.headers["X-Request-ID"]


class TestHealthProbes:
    def test_liveness_is_dependency_free(self, client, fake_redis):
        """A dependency check here would restart healthy pods during a Redis blip."""
        fake_redis.set_failing(True)

        response = client.get("/health/live")

        assert response.status_code == 200
        assert response.json()["status"] == "alive"

    def test_readiness_reports_checks(self, client):
        response = client.get("/health/ready")

        assert "redis" in response.json()["checks"]
        assert response.json()["idempotency_fail_mode"] in {"closed", "open"}

    def test_readiness_is_503_when_fail_closed_and_redis_is_down(self, client, monkeypatch):
        """Under fail_mode=closed a dead Redis means we cannot accept work safely."""

        class DeadRedis:
            @staticmethod
            def healthy() -> bool:
                return False

            @staticmethod
            def close() -> None:
                """The lifespan shutdown hook closes whatever it is handed."""

        assert api_main.settings.idempotency.fail_mode == "closed"
        monkeypatch.setattr(api_main, "get_redis_client", lambda: DeadRedis())

        response = client.get("/health/ready")

        assert response.status_code == 503
        assert response.json()["status"] == "degraded"


class TestAdminAccessControl:
    def test_consent_log_requires_admin_role(self, client):
        assert client.get("/admin/consent-log").status_code == 403

    def test_admin_can_read_consent_log(self, client):
        response = client.get("/admin/consent-log", headers={"x-role": "admin"})

        assert response.status_code == 200
        assert "consent_log" in response.json()

    def test_consent_log_is_populated_by_ingestion(self, client, dicom_payload):
        """Regression: the API used to write to its own dict while the admin
        endpoint read the worker's, so this view was permanently empty."""
        posted = client.post("/dicom/events", json=dicom_payload).json()

        log = client.get("/admin/consent-log", headers={"x-role": "admin"}).json()

        assert posted["consent_reference"] in log["consent_log"]


class TestUnhandledErrors:
    def test_traceback_never_reaches_the_client(self, monkeypatch, dicom_payload):
        """Locals in this service contain patient data by construction."""

        def exploding(_event):
            raise RuntimeError("boom: patient Jane Doe record 12345")

        monkeypatch.setattr(api_main, "publish_event", lambda e, **k: "msg-1")
        monkeypatch.setattr(api_main, "process_dicom_event", exploding)

        with TestClient(api_main.app, raise_server_exceptions=False) as client:
            response = client.post("/dicom/events", json=dicom_payload)

        assert response.status_code == 500
        assert "Jane Doe" not in response.text
        assert "Traceback" not in response.text
        assert response.json()["correlation_id"]
