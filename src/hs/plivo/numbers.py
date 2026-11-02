from hs.infra.http_session import read
from hs.infra.log import logger
from hs.infra.settings import settings
from hs.plivo.rest import reason


class NumberServiceError(Exception):
    pass


def get_plivo_number(auth_id: str, auth_token: str, number: str) -> dict:
    number = number.strip().replace(" ", "").removeprefix("+")
    url = f"{settings.plivo_api_url}/v1/Account/{auth_id}/Number/{number}/"
    headers = {"content-type": "application/json", "Accept": "application/json"}
    try:
        resp = read("GET", url, headers=headers, auth=(auth_id, auth_token), timeout=5)
    except Exception as e:
        logger.error(f"get_plivo_number: request to get plivo number failed: {e}")
        raise NumberServiceError(str(e))
    if resp.status_code != 200:
        logger.error(f"get_plivo_number: non 200 response status={resp.status_code} reason={reason(resp, auth_token)}")
        raise NumberServiceError("non 200 response")
    return resp.json()
