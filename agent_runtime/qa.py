from __future__ import annotations

from agent_runtime.contracts import QAGateMetrics, QAGateResult, QAGateThresholds


def evaluate_qa_gates(metrics: QAGateMetrics, thresholds: QAGateThresholds) -> QAGateResult:
    failures: list[str] = []
    if metrics.source_count < thresholds.min_sources:
        failures.append("sources_below_min")
    if metrics.signal_count < thresholds.min_signals:
        failures.append("signals_below_min")
    if metrics.contact_coverage_pct < thresholds.min_contact_coverage_pct:
        failures.append("contact_coverage_below_min")

    return QAGateResult(
        passed=not failures,
        failures=tuple(failures),
        thresholds=thresholds,
        metrics=metrics,
    )
