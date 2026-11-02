def test_the_plivo_clients_never_keep_a_cookie():
    """An upstream Set-Cookie is never stored, so it is never replayed on a later request from another thread."""
    import httpx

    from hs.infra.http_session import READS, WRITES

    request = httpx.Request("GET", "https://upstream.test/relayed")
    for client in (READS, WRITES):
        client.cookies.extract_cookies(httpx.Response(200, headers={"Set-Cookie": "sid=s; Path=/"}, request=request))
        assert len(client.cookies.jar) == 0
