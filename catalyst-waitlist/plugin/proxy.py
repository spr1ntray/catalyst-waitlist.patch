from __future__ import annotations

from urllib.parse import quote, urlparse


def proxy_to_url(raw: str) -> str:
    """Convert a Hub proxy into an httpx proxy URL.

    Accepts ``host:port:user:password`` and ``http(s)://`` or ``socks5://`` URLs.
    The password may itself contain colons.
    """
    value = (raw or "").strip()
    if not value:
        raise ValueError("empty_proxy")

    if "://" in value:
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https", "socks5", "socks5h"}:
            raise ValueError("invalid_proxy_scheme")
        if not parsed.hostname or not parsed.port:
            raise ValueError("invalid_proxy_url")
        if parsed.username is None:
            return f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
        user = quote(parsed.username, safe="")
        password = quote(parsed.password or "", safe="")
        return f"{parsed.scheme}://{user}:{password}@{parsed.hostname}:{parsed.port}"

    parts = value.split(":")
    if len(parts) < 2:
        raise ValueError("invalid_proxy")
    host, port = parts[0], parts[1]
    if not host or not port.isdigit() or not 1 <= int(port) <= 65535:
        raise ValueError("invalid_proxy")
    if len(parts) == 2:
        return f"http://{host}:{port}"
    if len(parts) == 4:
        user, password = parts[2], parts[3]
    elif len(parts) > 4:
        user, password = parts[2], ":".join(parts[3:])
    else:
        raise ValueError("invalid_proxy")
    if not user:
        raise ValueError("invalid_proxy")
    return f"http://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}"
