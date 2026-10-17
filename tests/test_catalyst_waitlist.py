from __future__ import annotations

import hashlib
import json
import threading
from typing import Any

import httpx

import plugin.main as main
from plugin.client import (
    ApiRejected,
    SafeRequestError,
    code_from_invite_link,
    idempotency_key,
    join_waitlist,
    public_twitter_handle,
)
from plugin.proxy import proxy_to_url
from soft_hub.sdk import HubAccount, HubContext, HubReferrals


def test_proxy_formats_hide_nothing_but_parse() -> None:
    assert proxy_to_url("198.51.100.10:8080") == "http://198.51.100.10:8080"
    assert (
        proxy_to_url("198.51.100.10:8080:user:p:ass")
        == "http://user:p%3Aass@198.51.100.10:8080"
    )
    assert proxy_to_url("http://user:secret@198.51.100.10:8080") == (
        "http://user:secret@198.51.100.10:8080"
    )


def test_handle_rejects_credentials() -> None:
    assert public_twitter_handle("@Ada_Lovelace") == "Ada_Lovelace"
    assert public_twitter_handle("name:password") is None
    assert public_twitter_handle("a" * 40) is None
    assert public_twitter_handle("") is None


def test_invite_link_accepts_only_official_r_param() -> None:
    assert code_from_invite_link("https://catalyst.app/?r=ab12") == "ab12"
    assert code_from_invite_link("https://evil.example/?r=ab12") is None
    assert code_from_invite_link("https://catalyst.app/?r=no") is None
    assert code_from_invite_link("https://catalyst.app/?r=ab12&r=cd34") is None


def test_join_sends_form_body_and_stable_key() -> None:
    seen: dict[str, Any] = {}

    def transport(**kwargs: Any) -> httpx.Response:
        seen.update(kwargs)
        request = httpx.Request("POST", kwargs["url"])
        return httpx.Response(
            200,
            json={"referralLink": "https://catalyst.app/?r=child1"},
            request=request,
        )

    result = join_waitlist(
        email="ada@example.com",
        proxy="198.51.100.10:8080:user:secret",
        twitter_handle="ada",
        referral_code="root1",
        timeout_seconds=5,
        transport=transport,
    )
    assert result.outcome == "joined"
    assert result.referral_code == "child1"
    assert seen["json"] == {
        "email": "ada@example.com",
        "twitterHandle": "ada",
        "referralCode": "root1",
    }
    assert seen["headers"]["Idempotency-Key"] == idempotency_key("ada@example.com")
    assert seen["proxy"].startswith("http://user:secret@")
    assert "PostHog" not in seen["headers"].get("User-Agent", "")
    assert "X-PostHog-Distinct-Id" not in seen["headers"]


def test_timeout_is_ambiguous_and_does_not_retry() -> None:
    calls = {"n": 0}

    def transport(**kwargs: Any) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ReadTimeout("slow")

    try:
        join_waitlist(
            email="ada@example.com",
            proxy="198.51.100.10:8080",
            twitter_handle=None,
            referral_code=None,
            timeout_seconds=5,
            transport=transport,
        )
    except SafeRequestError as error:
        assert error.code == "timeout"
        assert error.ambiguous is True
    else:
        raise AssertionError("timeout must fail closed")
    assert calls["n"] == 1


def test_chain_submits_parent_before_child_and_hides_codes() -> None:
    calls: list[dict[str, Any]] = []
    lock = threading.Lock()

    def transport(**kwargs: Any) -> httpx.Response:
        body = kwargs["json"]
        with lock:
            calls.append(body)
        email = body["email"]
        code = "c" + hashlib.sha256(email.encode()).hexdigest()[:7]
        request = httpx.Request("POST", kwargs["url"])
        return httpx.Response(
            200,
            json={"referralLink": f"https://catalyst.app/?r={code}"},
            request=request,
        )

    events: list[dict[str, Any]] = []
    context = _context(events, concurrency=2)
    original = main.join_waitlist

    def _join(**kwargs: Any):
        kwargs["transport"] = transport
        return original(**kwargs)

    main.join_waitlist = _join
    try:
        summary = main.run(context)
    finally:
        main.join_waitlist = original

    assert summary["succeeded"] == 4
    assert summary["failed"] == 0
    assert summary["blocked"] == 0
    assert summary["levels"] == 3
    by_email = {item["email"]: item for item in calls}
    assert "referralCode" not in by_email["root@example.com"]
    root_code = "c" + hashlib.sha256(b"root@example.com").hexdigest()[:7]
    child_code = "c" + hashlib.sha256(b"child@example.com").hexdigest()[:7]
    assert by_email["child@example.com"]["referralCode"] == root_code
    assert by_email["sibling@example.com"]["referralCode"] == root_code
    assert by_email["leaf@example.com"]["referralCode"] == child_code
    assert [item["email"] for item in calls].index("root@example.com") < [
        item["email"] for item in calls
    ].index("child@example.com")
    assert [item["email"] for item in calls].index("child@example.com") < [
        item["email"] for item in calls
    ].index("leaf@example.com")

    public = [event for event in events if event.get("type") != "protect_secret"]
    blob = json.dumps(public)
    assert root_code not in blob
    assert child_code not in blob
    assert "root@example.com" not in blob
    assert "secret-pass" not in blob
    assert "child_handle" not in blob
    assert any(event.get("type") == "protect_secret" for event in events)
    assert by_email["child@example.com"]["twitterHandle"] == "child_handle"
    assert "twitterHandle" not in by_email["sibling@example.com"]
    attached = [
        event["data"]["payload"]["referral_attached"]
        for event in events
        if event.get("type") == "result"
    ]
    assert attached.count(False) == 1
    assert attached.count(True) == 3


def test_child_without_parent_in_run_is_not_submitted() -> None:
    calls: list[str] = []

    def transport(**kwargs: Any) -> httpx.Response:
        calls.append(kwargs["json"]["email"])
        request = httpx.Request("POST", kwargs["url"])
        return httpx.Response(200, json={"referralLink": None}, request=request)

    events: list[dict[str, Any]] = []
    child = _account("child", "child@example.com", referrer="missing-parent", depth=1)
    context = _context_for(
        [child],
        events,
        parents=[_account("parent", "parent@example.com")],
        concurrency=1,
    )
    original = main.join_waitlist

    def _join(**kwargs: Any):
        kwargs["transport"] = transport
        return original(**kwargs)

    main.join_waitlist = _join
    try:
        summary = main.run(context)
    finally:
        main.join_waitlist = original
    assert calls == []
    assert summary["blocked"] == 1
    assert summary["succeeded"] == 0


def test_bad_referral_shape_is_rejected_before_send() -> None:
    def transport(**kwargs: Any) -> httpx.Response:
        raise AssertionError("must not send")

    try:
        join_waitlist(
            email="ada@example.com",
            proxy="198.51.100.10:8080",
            twitter_handle=None,
            referral_code="bad code",
            timeout_seconds=5,
            transport=transport,
        )
    except ApiRejected as error:
        assert error.code == "invalid_referral_code"
    else:
        raise AssertionError("expected reject")


def _context(events: list[dict[str, Any]], *, concurrency: int) -> HubContext:
    root = _account("root", "root@example.com", depth=0)
    child = _account("child", "child@example.com", referrer="root", depth=1, twitter_handle="child_handle")
    sibling = _account(
        "sibling",
        "sibling@example.com",
        referrer="root",
        depth=1,
        twitter_handle="name:secret-pass",
    )
    leaf = _account("leaf", "leaf@example.com", referrer="child", depth=2)
    return _context_for([root, child, sibling, leaf], events, parents=[root, child], concurrency=concurrency)


def _context_for(
    accounts: list[HubAccount],
    events: list[dict[str, Any]],
    *,
    parents: list[HubAccount],
    concurrency: int,
) -> HubContext:
    links = []
    for account in accounts:
        links.append(
            {
                "child_account_id": account.id,
                "parent_account_id": account.referrer_account_id,
                "depth": account.referral_depth,
            }
        )
    referrals = HubReferrals(
        {
            "mode": "project_runtime",
            "parents": [
                {"id": parent.id, "label": parent.label, "evm_address": parent.evm_address}
                for parent in parents
            ],
            "links": links,
        }
    )
    return HubContext(
        run_id="run-test",
        plugin_id="io.sprintray.catalyst-waitlist",
        plugin_version="1.0.0",
        action_id="join_waitlist",
        options={
            "timeout_seconds": 5,
            "account_pause_min_ms": 0,
            "account_pause_max_ms": 0,
            "level_pause_min_ms": 0,
            "level_pause_max_ms": 0,
        },
        accounts=tuple(accounts),
        plugin_root=".",
        scratch_dir=".",
        _emit_raw=events.append,
        _cancelled=threading.Event(),
        account_concurrency=concurrency,
        referral_mode="project_runtime",
        referrals=referrals,
    )


def _account(
    account_id: str,
    email: str,
    *,
    referrer: str | None = None,
    depth: int = 0,
    twitter_handle: str | None = None,
) -> HubAccount:
    payload: dict[str, Any] = {
        "id": account_id,
        "label": account_id,
        "evm_address": "0x" + "1" * 40,
        "email": email,
        "proxy": "198.51.100.10:8080:user:secret-pass",
        "referrer_account_id": referrer,
        "referral_depth": depth,
    }
    if twitter_handle is not None:
        payload["software_data"] = {"twitter_handle": twitter_handle}
    return HubAccount(payload)
