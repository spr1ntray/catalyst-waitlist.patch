from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

import httpx

from plugin.proxy import proxy_to_url

API_URL = "https://api.catalyst.app/api/v1/auth/waitlist"
SITE_ORIGIN = "https://catalyst.app"
SITE_HOST = "catalyst.app"

# Same shape the site stores from ?r= before it calls joinWaitlist.
REFERRAL_CODE_RE = re.compile(r"^[A-Za-z0-9_-]{4,64}$")
EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]{2,}$")
# Public X handle. Credentials (user:secret, auth tokens) do not match.
HANDLE_RE = re.compile(r"^[A-Za-z0-9_]{1,15}$")

# The recorded form POST carried a normal browser UA and no fingerprint body.
# This value stays fixed: the plugin does not rotate or synthesize device signals.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/143.0.0.0 Safari/537.36"
)

_ALREADY_CODES = frozenset(
    {
        "already_complete",
        "already_exists",
        "already_used",
        "conflict",
        "duplicate",
    }
)


class SafeRequestError(Exception):
    """Transport failure. ``ambiguous`` means the server may have accepted it."""

    def __init__(self, code: str, *, ambiguous: bool = False) -> None:
        self.code = code
        self.ambiguous = ambiguous
        super().__init__(code)


class ApiRejected(Exception):
    """The waitlist API refused the body. ``code`` is a stable token, not a message."""

    def __init__(self, code: str, http_status: int = 0) -> None:
        self.code = code
        self.http_status = http_status
        super().__init__(code)


@dataclass(frozen=True)
class JoinResult:
    http_status: int
    outcome: str
    referral_code: str | None
    twitter_sent: bool


def normalize_email(raw: str) -> str:
    email = (raw or "").strip().lower()
    if not EMAIL_RE.fullmatch(email) or len(email) > 320:
        raise ValueError("invalid_email")
    return email


def public_twitter_handle(raw: str | None) -> str | None:
    """Return a public handle, or None when the field is empty or is a credential."""
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if not value:
        return None
    if value.startswith("@"):
        value = value[1:].strip()
    if not HANDLE_RE.fullmatch(value):
        return None
    return value


def code_from_invite_link(link: Any) -> str | None:
    """Read ``r`` from ``https://catalyst.app/?r=...``. Anything else is ignored."""
    if not isinstance(link, str):
        return None
    parsed = urlparse(link.strip())
    if parsed.scheme != "https" or (parsed.hostname or "").lower() != SITE_HOST:
        return None
    values = parse_qs(parsed.query, keep_blank_values=False).get("r") or []
    if len(values) != 1:
        return None
    code = values[0].strip()
    if not REFERRAL_CODE_RE.fullmatch(code):
        return None
    return code


def idempotency_key(email: str) -> str:
    """Stable per email so a retry of the same signup is the same operation."""
    digest = hashlib.sha256(f"catalyst-waitlist:1:{email}".encode()).hexdigest()
    return digest


def join_waitlist(
    *,
    email: str,
    proxy: str,
    twitter_handle: str | None,
    referral_code: str | None,
    timeout_seconds: int,
    before_send: Callable[[], None] | None = None,
    transport: Callable[..., httpx.Response] | None = None,
) -> JoinResult:
    """POST the same JSON body the waitlist form sends. One attempt, no replay."""
    body: dict[str, str] = {"email": email}
    twitter_sent = False
    if twitter_handle:
        body["twitterHandle"] = twitter_handle
        twitter_sent = True
    if referral_code:
        if not REFERRAL_CODE_RE.fullmatch(referral_code):
            raise ApiRejected("invalid_referral_code")
        body["referralCode"] = referral_code

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": SITE_ORIGIN,
        "Referer": SITE_ORIGIN + "/",
        "Idempotency-Key": idempotency_key(email),
        "User-Agent": USER_AGENT,
    }
    if before_send is not None:
        before_send()

    try:
        response = _post(headers=headers, body=body, proxy=proxy, timeout_seconds=timeout_seconds, transport=transport)
    except SafeRequestError:
        raise
    except Exception:
        raise SafeRequestError("request_failed", ambiguous=True) from None

    return _parse_join(response, twitter_sent=twitter_sent)


def _post(
    *,
    headers: dict[str, str],
    body: dict[str, str],
    proxy: str,
    timeout_seconds: int,
    transport: Callable[..., httpx.Response] | None,
) -> httpx.Response:
    proxy_url = proxy_to_url(proxy)
    timeout = httpx.Timeout(timeout_seconds)
    try:
        if transport is not None:
            return transport(url=API_URL, headers=headers, json=body, proxy=proxy_url, timeout=timeout)
        with httpx.Client(proxy=proxy_url, timeout=timeout, trust_env=False, follow_redirects=False) as client:
            return client.post(API_URL, headers=headers, json=body)
    except httpx.TimeoutException as exc:
        ambiguous = not isinstance(exc, httpx.ConnectTimeout)
        raise SafeRequestError("timeout", ambiguous=ambiguous) from None
    except httpx.ConnectError:
        raise SafeRequestError("proxy_unavailable", ambiguous=False) from None
    except httpx.HTTPError:
        raise SafeRequestError("request_failed", ambiguous=True) from None


def _parse_join(response: httpx.Response, *, twitter_sent: bool) -> JoinResult:
    status = int(response.status_code)
    if status in {301, 302, 303, 307, 308}:
        raise ApiRejected("unexpected_redirect", status)
    payload = _json_object(response)
    code_name = _error_code(payload)
    referral = code_from_invite_link(payload.get("referralLink") if isinstance(payload, dict) else None)

    if status == 200 and referral:
        outcome = "already_listed" if code_name in _ALREADY_CODES else "joined"
        return JoinResult(status, outcome, referral, twitter_sent)
    if status == 200 and isinstance(payload, dict) and payload.get("referralLink") is None and code_name is None:
        return JoinResult(status, "joined_without_link", None, twitter_sent)
    if status in {200, 409} and code_name in _ALREADY_CODES and referral:
        return JoinResult(status, "already_listed", referral, twitter_sent)
    if status == 409 and code_name in _ALREADY_CODES:
        return JoinResult(status, "already_listed", None, twitter_sent)
    if code_name:
        raise ApiRejected(code_name, status)
    if status == 400:
        raise ApiRejected("bad_request", status)
    if status == 429:
        raise ApiRejected("rate_limited", status)
    if status in {401, 403}:
        raise ApiRejected("forbidden", status)
    if 500 <= status <= 599:
        raise ApiRejected("server_error", status)
    raise ApiRejected("unexpected_http_status", status)


def _json_object(response: httpx.Response) -> dict[str, Any]:
    raw = response.content
    if len(raw) > 65536:
        raise ApiRejected("invalid_response", int(response.status_code))
    if not raw:
        return {}
    try:
        parsed = response.json()
    except Exception:
        raise ApiRejected("invalid_response", int(response.status_code)) from None
    if not isinstance(parsed, dict):
        raise ApiRejected("invalid_response", int(response.status_code))
    return parsed


def _error_code(payload: dict[str, Any]) -> str | None:
    raw = payload.get("code")
    if not isinstance(raw, str):
        return None
    token = raw.strip().lower()
    if not re.fullmatch(r"[a-z0-9_]{1,64}", token):
        return None
    return token
