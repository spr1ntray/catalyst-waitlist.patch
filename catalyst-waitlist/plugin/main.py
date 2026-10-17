from __future__ import annotations

import random
import threading
import time
from typing import Any, Callable

from soft_hub.sdk import CancelledError, HubAccount, HubContext

from plugin.client import (
    ApiRejected,
    JoinResult,
    SafeRequestError,
    join_waitlist,
    normalize_email,
    public_twitter_handle,
)
from plugin.proxy import proxy_to_url

_TABLE_KEYS = ("joined", "referral_attached", "twitter_sent", "outcome")


class _ParentCodeError(Exception):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _CodeCache:
    """In-memory invite codes for this run. Cleared when the action returns."""

    def __init__(self) -> None:
        self._codes: dict[str, str] = {}
        self._lock = threading.Lock()

    def put(self, account_id: str, code: str) -> None:
        with self._lock:
            self._codes[account_id] = code

    def get(self, account_id: str) -> str | None:
        with self._lock:
            return self._codes.get(account_id)

    def clear(self) -> None:
        with self._lock:
            self._codes.clear()


def run(context: HubContext) -> dict[str, Any]:
    if context.action_id != "join_waitlist":
        raise ValueError("unsupported_action")
    return _join(context)


def _join(context: HubContext) -> dict[str, Any]:
    timeout_seconds = _int_option(context.options, "timeout_seconds", 30, 5, 120)
    account_lo, account_hi = _ms_range(
        context.options,
        "account_pause_min_ms",
        "account_pause_max_ms",
        default_lo=400,
        default_hi=1200,
        high=30000,
    )
    level_lo, level_hi = _ms_range(
        context.options,
        "level_pause_min_ms",
        "level_pause_max_ms",
        default_lo=800,
        default_hi=2000,
        high=60000,
    )

    selected = {account.id for account in context.accounts}
    cache = _CodeCache()
    finished: set[str] = set()
    counters = {
        "total": len(context.accounts),
        "succeeded": 0,
        "failed": 0,
        "blocked": 0,
        "cancelled": 0,
        "levels": 0,
    }
    lock = threading.Lock()
    levels = _levels(context)
    counters["levels"] = len(levels)

    context.log(
        "Старт записи в waitlist Catalyst",
        data={
            "accounts": len(context.accounts),
            "levels": len(levels),
            "account_concurrency": int(context.account_concurrency),
            "account_pause_ms": [account_lo, account_hi],
            "level_pause_ms": [level_lo, level_hi],
            "timeout_seconds": timeout_seconds,
        },
    )

    def worker(account: HubAccount) -> str:
        try:
            status = _process_account(
                context,
                account,
                timeout_seconds=timeout_seconds,
                cache=cache,
                selected_ids=selected,
                account_pause=_pause_range(account_lo, account_hi),
            )
        except CancelledError:
            with lock:
                finished.add(account.id)
                counters["cancelled"] += 1
            raise
        with lock:
            finished.add(account.id)
            counters[status] = counters.get(status, 0) + 1
        return status

    try:
        for index, level in enumerate(levels):
            context.check_cancelled()
            context.log(
                "Уровень реферальной цепи",
                data={"level": index, "accounts": len(level)},
            )
            context.map_accounts(worker, accounts=level)
            if index + 1 < len(levels) and (level_lo or level_hi):
                chosen = random.randint(level_lo, level_hi)
                if chosen:
                    context.log(
                        "Пауза перед следующим уровнем цепи",
                        data={"level_pause_ms": chosen, "from_level": index},
                    )
                    _sleep(context, chosen / 1000)
    except CancelledError:
        for account in context.accounts:
            if account.id in finished:
                continue
            context.account_state(
                account.id,
                status="cancelled",
                stage="cancelled",
                message="Остановлено до обработки аккаунта",
            )
            with lock:
                counters["cancelled"] += 1
        raise
    finally:
        cache.clear()

    return {
        "total": counters["total"],
        "succeeded": counters.get("succeeded", 0),
        "failed": counters.get("failed", 0),
        "blocked": counters.get("blocked", 0),
        "cancelled": counters.get("cancelled", 0),
        "levels": counters["levels"],
    }


def _process_account(
    context: HubContext,
    account: HubAccount,
    *,
    timeout_seconds: int,
    cache: _CodeCache,
    selected_ids: set[str],
    account_pause: Callable[[HubContext], None],
) -> str:
    context.check_cancelled()
    account_pause(context)
    context.check_cancelled()
    context.account_state(
        account.id,
        status="running",
        stage="preflight",
        progress=0.05,
        message="Проверяем email и proxy",
    )

    try:
        email = normalize_email(account.secret("email"))
        proxy = account.secret("proxy")
        proxy_to_url(proxy)
    except (KeyError, ValueError):
        return _finish(
            context,
            account,
            status="blocked",
            stage="resource_missing",
            message="Нет корректных email или proxy",
            outcome="blocked",
        )

    handle = _optional_handle(account)
    context.account_state(
        account.id,
        status="running",
        stage="referral",
        progress=0.25,
        message="Сверяем место в реферальной цепи",
    )

    try:
        referral_code = _referral_code_for(context, account, cache=cache, selected_ids=selected_ids)
    except _ParentCodeError as error:
        return _finish(
            context,
            account,
            status="blocked",
            stage="referral",
            message=_parent_message(error.code),
            outcome="blocked",
        )

    write_may_have_happened = False
    try:
        context.check_cancelled()
        context.account_state(
            account.id,
            status="running",
            stage="submitting",
            progress=0.45,
            message="Отправляем заявку waitlist" if referral_code is None else "Отправляем заявку с кодом родителя",
        )

        def before_send() -> None:
            nonlocal write_may_have_happened
            context.check_cancelled()
            write_may_have_happened = True

        result = join_waitlist(
            email=email,
            proxy=proxy,
            twitter_handle=handle,
            referral_code=referral_code,
            timeout_seconds=timeout_seconds,
            before_send=before_send,
        )
        context.account_state(
            account.id,
            status="running",
            stage="response_received",
            progress=0.75,
            message="Ответ waitlist получен",
        )
        _remember_code(context, cache, account.id, result.referral_code)
        context.account_state(
            account.id,
            status="running",
            stage="response_validated",
            progress=0.9,
            message="Ответ заявки проверен",
        )
    except CancelledError:
        context.account_state(
            account.id,
            status="cancelled",
            stage="external_outcome_unknown" if write_may_have_happened else "cancelled",
            message=(
                "Остановлено после отправки. Проверьте почту на catalyst.app перед повтором"
                if write_may_have_happened
                else "Остановлено до отправки заявки"
            ),
        )
        raise
    except SafeRequestError as error:
        ambiguous = error.ambiguous or write_may_have_happened
        return _finish(
            context,
            account,
            status="failed",
            stage="external_outcome_unknown" if ambiguous else "request_failed",
            message=(
                "Нет ответа после отправки. Проверьте почту на catalyst.app перед повтором"
                if ambiguous
                else "Заявка не ушла: прокси или сеть недоступны"
            ),
            outcome="failed",
            referral_attached=referral_code is not None,
            twitter_sent=handle is not None,
        )
    except ApiRejected as error:
        blocked = error.code in {"invalid_email", "invalid_twitter_handle", "invalid_referral_code"}
        return _finish(
            context,
            account,
            status="blocked" if blocked else "failed",
            stage="rejected" if not write_may_have_happened else "external_outcome_unknown",
            message=_reject_message(error.code, write_may_have_happened),
            outcome="blocked" if blocked else "failed",
            referral_attached=referral_code is not None,
            twitter_sent=handle is not None,
        )
    except Exception:
        return _finish(
            context,
            account,
            status="failed",
            stage="external_outcome_unknown" if write_may_have_happened else "automation_failed",
            message=(
                "Ошибка после отправки. Проверьте почту на catalyst.app перед повтором"
                if write_may_have_happened
                else "Заявка не выполнена"
            ),
            outcome="failed",
            referral_attached=referral_code is not None,
            twitter_sent=handle is not None,
        )

    return _finish(
        context,
        account,
        status="succeeded",
        stage="completed",
        message=_success_message(result.outcome),
        outcome=result.outcome,
        joined=1 if result.outcome in {"joined", "already_listed", "joined_without_link"} else 0,
        referral_attached=referral_code is not None,
        twitter_sent=result.twitter_sent,
        progress=1.0,
    )


def _optional_handle(account: HubAccount) -> str | None:
    """Public handle from optional software data. A missing field does not block."""
    try:
        raw = account.software_data.secret("twitter_handle")
    except KeyError:
        return None
    return public_twitter_handle(raw)


def _referral_code_for(
    context: HubContext,
    child: HubAccount,
    *,
    cache: _CodeCache,
    selected_ids: set[str],
) -> str | None:
    """Parent code from this run, or None for a root. Never invent a root."""
    try:
        parent = context.referrals.parent_for(child.id)
    except KeyError as exc:
        raise _ParentCodeError("parent_unavailable") from exc
    if parent is None:
        return None
    if parent.id not in selected_ids:
        raise _ParentCodeError("parent_not_in_run")
    code = cache.get(parent.id)
    if not code:
        raise _ParentCodeError("parent_code_missing")
    return context.protect_secret(code)


def _remember_code(
    context: HubContext,
    cache: _CodeCache,
    account_id: str,
    code: str | None,
) -> None:
    if not code:
        return
    protected = context.protect_secret(code)
    cache.put(account_id, protected)


def _levels(context: HubContext) -> tuple[tuple[HubAccount, ...], ...]:
    levels = tuple(tuple(level) for level in context.referral_levels)
    if levels:
        return levels
    return (tuple(context.accounts),)


def _finish(
    context: HubContext,
    account: HubAccount,
    *,
    status: str,
    stage: str,
    message: str,
    outcome: str,
    joined: int = 0,
    referral_attached: bool = False,
    twitter_sent: bool = False,
    progress: float | None = None,
) -> str:
    if status != "cancelled":
        data = {
            "joined": joined,
            "referral_attached": referral_attached,
            "twitter_sent": twitter_sent,
            "outcome": outcome,
        }
        if tuple(data) != _TABLE_KEYS:
            raise RuntimeError("table_schema_mismatch")
        context.result(
            f"{account.label}: {message}",
            kind="account_snapshot",
            status=status,
            account_id=account.id,
            data=data,
        )
    context.account_state(
        account.id,
        status=status,
        stage=stage,
        progress=progress,
        message=message,
    )
    return status


def _parent_message(code: str) -> str:
    if code == "parent_not_in_run":
        return "Родитель не в этом запуске. Добавьте его в ту же задачу, выше по цепи"
    if code == "parent_code_missing":
        return "Родитель не отдал invite-код. Сначала он должен успешно встать в waitlist"
    return "Родитель из цепи Hub недоступен этому запуску"


def _success_message(outcome: str) -> str:
    if outcome == "already_listed":
        return "Email уже в waitlist"
    if outcome == "joined_without_link":
        return "Заявка принята, invite-ссылка в ответе не пришла"
    return "Заявка в waitlist принята"


def _reject_message(code: str, ambiguous: bool) -> str:
    if ambiguous and code in {"server_error", "unexpected_http_status", "invalid_response"}:
        return "Ответ сервиса неясен. Проверьте почту на catalyst.app перед повтором"
    mapping = {
        "invalid_email": "Сервис не принял email",
        "invalid_twitter_handle": "Сервис не принял Twitter handle",
        "invalid_referral_code": "Код родителя не подошёл по формату проекта",
        "bad_request": "Сервис отклонил заявку",
        "rate_limited": "Слишком много запросов. Уменьшите число потоков",
        "forbidden": "Сервис запретил заявку",
        "server_error": "Сервис временно недоступен",
        "unexpected_redirect": "Сервис ответил редиректом вместо заявки",
        "invalid_response": "Сервис вернул непонятный ответ",
        "unexpected_http_status": "Сервис вернул неожиданный ответ",
    }
    return mapping.get(code, "Заявка не принята")


def _int_option(options: dict[str, Any], name: str, default: int, low: int, high: int) -> int:
    value = options.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"invalid_{name}")
    if not low <= value <= high:
        raise ValueError(f"invalid_{name}")
    return value


def _ms_range(
    options: dict[str, Any],
    low_name: str,
    high_name: str,
    *,
    default_lo: int,
    default_hi: int,
    high: int,
) -> tuple[int, int]:
    low = _int_option(options, low_name, default_lo, 0, high)
    upper = _int_option(options, high_name, default_hi, 0, high)
    if low > upper:
        raise ValueError(f"invalid_{low_name}")
    return low, upper


def _pause_range(low_ms: int, high_ms: int) -> Callable[[HubContext], None]:
    def wait(context: HubContext) -> None:
        if high_ms <= 0:
            return
        _sleep(context, random.randint(low_ms, high_ms) / 1000)

    return wait


def _sleep(context: HubContext, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while True:
        context.check_cancelled()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(0.2, remaining))


