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


def main() -> None:
    test_policy_engine_accepts_bounded_control_apply()
    test_policy_engine_rejects_missing_reason_and_invalid_control()
    print("test_agent_policy: ok")


if __name__ == "__main__":
    main()
