from __future__ import annotations

import json
import tempfile
from pathlib import Path

import cli.agent_runtime_ops as ops


def test_run_agent_external_research_completes_package() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        out_dir = root / "out"
        lead_root = out_dir / "lead_intelligence"
        package_dir = lead_root / "leads" / "disp001-demo"
        package_dir.mkdir(parents=True, exist_ok=True)

        (lead_root / "lead_intelligence_manifest.json").write_text(
            json.dumps(
                {
                    "packages": [
                        {
                            "company_name": "Demo Co",
                            "lead_id": "DISP001",
                            "package_dir": "leads/disp001-demo",
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        (package_dir / "lead_summary.json").write_text(
            json.dumps({"company_name": "Demo Co", "website": "https://example.com", "location_count": 1}),
            encoding="utf-8",
        )
        (package_dir / "agent_research_prompt.md").write_text("Find public data", encoding="utf-8")
        (package_dir / "external_research_status.json").write_text(json.dumps({"status": "pending"}), encoding="utf-8")

        config_path = root / "config" / "agent_runtime.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            json.dumps(
                {
                    "enabled": True,
                    "provider_modes": {
                        "openai_api": {"available": False},
                        "codex_auth": {"available": True},
                        "clawbot": {"available": False},
                    },
                    "model_role_slots": {
                        "research": {
                            "model": "test-model",
                            "preferred_providers": ["codex_auth"],
                        }
                    },
                }
            ),
            encoding="utf-8",
        )

        original_invoke = ops.invoke_provider_mode
        try:
            ops.invoke_provider_mode = lambda mode, model, call: {  # type: ignore[assignment]
                "provider_mode": mode,
                "model": model,
                "text": "# External Research - Demo Co\n\n## Company Overview\n- ok\n\n## Sources\n1. https://example.com",
            }
            result = ops.run_agent_external_research(out_dir=str(out_dir), config_path=str(config_path), limit=0)
        finally:
            ops.invoke_provider_mode = original_invoke  # type: ignore[assignment]

        assert result["completed"] == 1
        status = json.loads((package_dir / "external_research_status.json").read_text(encoding="utf-8"))
        assert status["status"] == "completed"
        assert status["source_count"] >= 1
        assert (package_dir / "external_research_report.md").exists()
