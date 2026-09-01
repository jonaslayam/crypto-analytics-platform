"""Tests for CoincapExtractor.

Rewritten against the actual current API: the previous version of this
file imported `get_coincap_assets_and_save` and patched `BASE_DIR`,
neither of which exist anymore -- extract.py was refactored into the
CoincapExtractor class (RAW_DATA_DIR module constant, .extract()
method) at some point and this test was never updated, so it couldn't
even be collected by pytest (ImportError). There was also no CI to
catch that.
"""
import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import requests
import requests_mock

from src.extract import CoincapExtractor

MOCK_RESPONSE = {
    "data": [
        {"id": "bitcoin", "name": "Bitcoin", "priceUsd": "60000"},
        {"id": "ethereum", "name": "Ethereum", "priceUsd": "3000"},
    ],
    "timestamp": int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000),
}
API_URL = "https://rest.coincap.io/v3/assets"


class FakeObjectStorageClient:
    """Records what would have been uploaded instead of calling OCI."""
    def __init__(self):
        self.uploads = []

    def put_object(self, namespace_name, bucket_name, object_name, put_object_body):
        self.uploads.append({
            "namespace": namespace_name,
            "bucket": bucket_name,
            "object_name": object_name,
            "body": put_object_body.read(),
        })


@pytest.fixture
def extractor(monkeypatch, tmp_path):
    monkeypatch.setattr("src.extract.RAW_DATA_DIR", tmp_path)
    monkeypatch.setattr("src.extract.API_URL", API_URL)
    ext = CoincapExtractor(api_key="test-key", limit=2)
    ext.namespace = "test-namespace"
    ext.bucket_name = "test-bucket"
    return ext


def test_extract_uploads_the_api_response_under_a_hive_date_partition(extractor):
    fake_client = FakeObjectStorageClient()

    with requests_mock.Mocker() as m, \
         patch("src.extract.get_object_storage_client", return_value=fake_client):
        m.get(API_URL, json=MOCK_RESPONSE)
        oci_uri = extractor.extract()

    assert len(fake_client.uploads) == 1
    upload = fake_client.uploads[0]

    assert upload["namespace"] == "test-namespace"
    assert upload["bucket"] == "test-bucket"
    assert upload["object_name"].startswith("raw/year=2026/month=01/day=01/assets_")
    assert oci_uri == f"oci://test-bucket@test-namespace/{upload['object_name']}"

    body = json.loads(upload["body"])
    assert body["data"][0]["id"] == "bitcoin"
    assert body["data"][1]["priceUsd"] == "3000"


def test_extract_sends_the_api_key_as_a_bearer_token(extractor):
    with requests_mock.Mocker() as m, \
         patch("src.extract.get_object_storage_client", return_value=FakeObjectStorageClient()):
        m.get(API_URL, json=MOCK_RESPONSE)
        extractor.extract()

    assert m.last_request.headers["Authorization"] == "Bearer test-key"


def test_extract_raises_on_http_error_and_does_not_upload(extractor):
    with requests_mock.Mocker() as m, \
         patch("src.extract.get_object_storage_client") as get_client:
        m.get(API_URL, status_code=500)
        with pytest.raises(requests.exceptions.HTTPError):
            extractor.extract()

    get_client.assert_not_called()


def test_extract_raises_a_clear_error_when_the_response_has_no_timestamp(extractor):
    """Guards against a bare TypeError from `None > 1e10` if CoinCap ever
    changes its response shape -- fails with a message that says why."""
    with requests_mock.Mocker() as m, \
         patch("src.extract.get_object_storage_client") as get_client:
        m.get(API_URL, json={"data": []})
        with pytest.raises(ValueError, match="no 'timestamp' field"):
            extractor.extract()

    get_client.assert_not_called()


def test_extract_cleans_up_the_local_temp_file_even_when_upload_fails(extractor):
    from src import extract as extract_module

    with requests_mock.Mocker() as m, \
         patch("src.extract.get_object_storage_client", side_effect=RuntimeError("simulated OCI failure")):
        m.get(API_URL, json=MOCK_RESPONSE)
        with pytest.raises(RuntimeError):
            extractor.extract()

    assert list(extract_module.RAW_DATA_DIR.glob("assets_*.json")) == []
