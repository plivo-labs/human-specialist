def test_status_reports_only_db_and_redis_booleans(client):
    resp = client.get("/status")
    assert resp.status_code == 200
    assert resp.json() == {"db": True, "redis": True}
