import pytest
from fastapi.testclient import TestClient
from src.api.main import app

client = TestClient(app)

class TestAPI:
    def test_root(self):
        response = client.get("/")
        assert response.status_code == 200
        assert "name" in response.json()

    def test_health(self):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"

    def test_list_jobs(self):
        response = client.get("/api/v1/jobs")
        assert response.status_code == 200
        data = response.json()
        assert "items" in data
        assert "total" in data

    def test_trending_skills(self):
        response = client.get("/api/v1/skills/trending")
        assert response.status_code == 200
        assert isinstance(response.json(), list)