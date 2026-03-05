import json
import pytest
import requests_mock
from pathlib import Path
from src.extract import get_coincap_assets_and_save

MOCK_RESPONSE = {
    "data": [
        {"id": "bitcoin", "name": "Bitcoin", "priceUsd": "60000"},
        {"id": "ethereum", "name": "Ethereum", "priceUsd": "3000"}
    ],
    "timestamp": 123456789
}

@pytest.fixture
def mock_api():
    """Fixture to mock the Coincap API."""
    with requests_mock.Mocker() as m:
        m.get("https://rest.coincap.io/v3/assets", json=MOCK_RESPONSE)
        yield m

def test_get_coincap_assets(mock_api, tmp_path, monkeypatch):
    """Test that the Coincap API is called and data is extracted correctly."""
    test_raw_dir = tmp_path / "data" / "raw"
    test_raw_dir.mkdir(parents=True)
    monkeypatch.setattr("src.extract.BASE_DIR", tmp_path)

    assets = get_coincap_assets_and_save(limit=2)

    assert isinstance(assets, Path)
    assert assets.exists()
    assert assets.name.startswith("assets_")
    assert assets.name.endswith(".json")

    with open(assets, 'r', encoding='utf-8') as f:
        content = json.load(f)
    
    assert "data" in content
    assert len(content["data"]) == 2
    assert content["data"][0]["id"] == "bitcoin"
    assert content["data"][0]["priceUsd"] == "60000"