#!/usr/bin/env python3.11
from __future__ import annotations

from agent_runtime.policy import PolicyEngine
from cli.errors import DataValidationError


def test_policy_engine_accepts_bounded_control_apply() -> None:
    policy = PolicyEngine()
    policy.validate(
        "control_apply",
        {
            "reason": "Stop a blocked domain",
            "action": "stop-domain",
            "domain": "blocked.example",
        },
    )


def test_policy_engine_rejects_missing_reason_and_invalid_control() -> None:
    policy = PolicyEngine()
    try:
        policy.validate("status", {"reason": ""})
        raise AssertionError("missing reason should fail")
    except DataValidationError:
        pass


def test_policy_engine_rejects_unbounded_sync_arguments() -> None:
    policy = PolicyEngine()
    for arguments in (
        {"reason": "broad run", "crawl_mode": "full", "max": 2, "limit": 15},
        {"reason": "too many seeds", "crawl_mode": "refresh", "max": 10, "limit": 15},
        {"reason": "too much export", "crawl_mode": "refresh", "max": 3, "limit": 100},
    ):
        try:
            policy.validate("sync", arguments)
            raise AssertionError("invalid sync arguments should fail")
        except DataValidationError:
            pass


def test_policy_engine_accepts_small_refresh_sync() -> None:
    policy = PolicyEngine()
    policy.validate(
        "sync",
        {
            "reason": "bounded smoke run",
            "crawl_mode": "refresh",
            "max": 3,
            "limit": 15,
        },
    )

    try:
        policy.validate(
            "control_apply",
            {
                "reason": "bad cap",
                "action": "cap-domain",
                "domain": "blocked.example",
                "max_pages": 0,
            },
        )
        raise AssertionError("non-positive cap should fail")
    except DataValidationError:
        pass


def test_policy_engine_allows_read_only_pragma_sql() -> None:
    policy = PolicyEngine()
    policy.validate(
        "sql",
        {
            "reason": "inspect schema",
            "query": "PRAGMA table_info(review_queue)",
        },
    )


def main() -> None:
    test_policy_engine_accepts_bounded_control_apply()
    test_policy_engine_rejects_missing_reason_and_invalid_control()
    test_policy_engine_rejects_unbounded_sync_arguments()
    test_policy_engine_accepts_small_refresh_sync()
    test_policy_engine_allows_read_only_pragma_sql()
    print("test_agent_policy: ok")


if __name__ == "__main__":
    main()
