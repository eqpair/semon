import aiohttp
import asyncio
import json
import logging
import os
import time
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent / ".env")

APP_KEY    = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
BASE_URL   = "https://openapi.koreainvestment.com:9443"
TOKEN_PATH = Path(__file__).parent / ".kis_token.json"
TTL        = 86400 - 300

logger = logging.getLogger(__name__)
_lock  = asyncio.Lock()


def _load_token() -> tuple[str, float] | tuple[None, None]:
    try:
        with open(TOKEN_PATH) as f:
            d = json.load(f)
        return d["token"], d["issued_at"]
    except Exception:
        return None, None


def _save_token(token: str, issued_at: float):
    with open(TOKEN_PATH, "w") as f:
        json.dump({"token": token, "issued_at": issued_at}, f)


async def get_access_token() -> str:
    async with _lock:
        token, issued_at = _load_token()
        if token and (time.time() - issued_at) < TTL:
            return token
        async with aiohttp.ClientSession() as session:
            resp = await session.post(
                f"{BASE_URL}/oauth2/tokenP",
                json={
                    "grant_type": "client_credentials",
                    "appkey":     APP_KEY,
                    "appsecret":  APP_SECRET,
                }
            )
            data = await resp.json()
        token = data["access_token"]
        issued_at = time.time()
        _save_token(token, issued_at)
        logger.info("KIS access_token 갱신 완료")
        return token