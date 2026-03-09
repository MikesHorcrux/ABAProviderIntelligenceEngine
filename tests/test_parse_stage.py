#!/usr/bin/env python3.11
from __future__ import annotations

from pipeline.fetch_backends.common import FetchResult
from pipeline.stages.discovery import DiscoverySeed
from pipeline.stages.extract import extract_records


def test_extract_records_builds_provider_intelligence_signal() -> None:
    html = """
        <html>
        <head><title>Garden State Psychology Center</title></head>
        <body>
        <h1>Garden State Psychology Center</h1>
        <p>Dr. Jane Smith, PsyD provides autism diagnostic evaluations and ADHD assessment services.</p>
        <p>Call (973) 555-0112. Fax: (973) 555-0113.</p>
        <p>Telehealth available for follow-up visits.</p>
        <p>We work with children, adolescents, and adults.</p>
        <a href="/intake">New patient intake</a>
        </body>
        </html>
    """
    fetch = FetchResult(
        job_pk="job_demo",
        seed_name="Garden State Psychology Center",
        seed_state="NJ",
        seed_market="Newark",
        seed_website="https://gardenstate.example",
        target_url="https://gardenstate.example/providers",
        normalized_url="https://gardenstate.example/providers",
        status_code=200,
        content=html,
        content_hash="hash",
        fetched_at="2026-03-09T00:00:00Z",
    )
    seed = DiscoverySeed(
        name="Garden State Psychology Center",
        website="https://gardenstate.example",
        state="NJ",
        market="Newark",
        tier="C",
        source_type="practice_site",
        extraction_profile="practice",
    )
    extracted = extract_records(fetch, seed, {"newark": "Newark"})
    assert len(extracted) == 1
    row = extracted[0]
    assert row.provider_name == "Jane Smith"
    assert row.credentials == "PsyD"
    assert row.practice_name == "Garden State Psychology Center"
    assert row.diagnoses_asd == "yes"
    assert row.diagnoses_adhd == "yes"
    assert row.telehealth == "yes"
    assert "child" in row.age_groups
    assert row.intake_url.endswith("/intake")


def main() -> None:
    test_extract_records_builds_provider_intelligence_signal()
    print("test_parse_stage: ok")


if __name__ == "__main__":
    main()
