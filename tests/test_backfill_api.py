"""API wiring tests for POST /backfill.

Skipped when FastAPI/httpx are not installed: the core backfill suite stays
runnable with only the standard library via ``python3 -I tests/run.py``.
"""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:  # pragma: no cover - environment dependent
    from fastapi.testclient import TestClient

    from app.main import app
    from app.admission.backfill import BackfillService
    from app.admission.config import Settings
    from tests.fake_backfill import (
        FakeBackfillTransport,
        install_index_fixture,
    )

    _DEPS_AVAILABLE = True
    SETTINGS = Settings(
        allowed_registries=("localhost:5000", "registry:5000"),
        registry_upstream="http://registry.invalid",
        request_timeout=1.0,
    )
except Exception:  # noqa: BLE001 - any missing dep means skip
    _DEPS_AVAILABLE = False

TARGET = "images/target"
GOLDEN = "images/golden"


@unittest.skipUnless(_DEPS_AVAILABLE, "fastapi/httpx not installed")
class BackfillApiTests(unittest.TestCase):
    def setUp(self):
        import app.main as main

        self.main = main
        self.transport = FakeBackfillTransport()
        self.fixture = install_index_fixture(self.transport, TARGET)
        for digest, size in self.fixture["layers"].values():
            self.transport.add_blob(GOLDEN, digest, size)
            self.transport.remove_blob(TARGET, digest)
        self.service = BackfillService(SETTINGS, transport=self.transport)
        self._old_service = main.backfill_service
        main.backfill_service = self.service
        self.client = TestClient(app)

    def tearDown(self):
        self.main.backfill_service = self._old_service

    def _target_body(self, **overrides):
        body = {
            "target": (
                "localhost:5000/images/target@" + self.fixture["index_digest"]
            ),
            "sources": [GOLDEN],
        }
        body.update(overrides)
        return body

    def test_backfill_endpoint_reports_complete(self):
        response = self.client.post("/backfill", json=self._target_body())
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["complete"])
        self.assertEqual(payload["summary"]["failed"], 0)
        self.assertEqual(payload["summary"]["layers"], 3)
        self.assertNotIn("detail", payload)

    def test_backfill_endpoint_reports_incomplete_with_failures(self):
        response = self.client.post(
            "/backfill", json=self._target_body(sources=["images/nothing"])
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["complete"])
        self.assertEqual(payload["summary"]["failed"], 3)
        self.assertEqual(len(payload["failures"]), 3)
        self.assertTrue(payload["failures"][0]["reasons"])

    def test_alias_field_names_accepted(self):
        response = self.client.post(
            "/backfill",
            json={
                "target_reference": self._target_body()["target"],
                "source_repositories": [GOLDEN],
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["complete"])

    def test_versioned_path_alias_works(self):
        response = self.client.post(
            "/api/v1/backfill", json=self._target_body()
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["complete"])

    def test_missing_target_body_is_422(self):
        response = self.client.post("/backfill", json={"sources": []})
        self.assertEqual(response.status_code, 422)

    def test_tag_target_is_400_with_detail(self):
        response = self.client.post(
            "/backfill",
            json={"target": "localhost:5000/images/target:v1",
                  "sources": [GOLDEN]},
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("摘要", response.json()["detail"])

    def test_unknown_index_is_404_with_detail(self):
        response = self.client.post(
            "/backfill",
            json={
                "target": "localhost:5000/images/target@"
                + "sha256:" + "a" * 64,
                "sources": [],
            },
        )
        self.assertEqual(response.status_code, 404)
        self.assertIn("detail", response.json())

    def test_mutate_route_still_mounted(self):
        # Preserving /mutate is an explicit requirement.
        response = self.client.post("/mutate", json={})
        # Malformed/empty AdmissionReview is denied with 200 + allowed=false,
        # not 404: the route and its behavior survive.
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["response"]["allowed"])


if __name__ == "__main__":
    unittest.main()
