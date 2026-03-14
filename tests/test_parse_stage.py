#!/usr/bin/env python3.11
from __future__ import annotations

from pathlib import Path

from pipeline.fetch_backends.common import FetchResult
from pipeline.stages.discovery import DiscoverySeed
from pipeline.stages.extract import _classify_page, _provider_candidates, extract_records


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "provider_intel"


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


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


def test_extract_records_skips_blocked_board_page() -> None:
    fetch = FetchResult(
        job_pk="job_board",
        seed_name="NJ BME Physician Search",
        seed_state="NJ",
        seed_market="statewide",
        seed_website="https://www.njconsumeraffairs.gov/bme/Pages/default.aspx",
        target_url="https://www.njconsumeraffairs.gov/bme/Pages/default.aspx",
        normalized_url="https://www.njconsumeraffairs.gov/bme/Pages/default.aspx",
        status_code=200,
        content=_fixture("nj_board_protected.html"),
        content_hash="hash_board",
        fetched_at="2026-03-09T00:00:00Z",
    )
    seed = DiscoverySeed(
        name="NJ BME Physician Search",
        website="https://www.njconsumeraffairs.gov/bme/Pages/default.aspx",
        state="NJ",
        market="statewide",
        tier="A",
        source_type="licensing_board",
        extraction_profile="board",
    )
    extracted = extract_records(fetch, seed, {})
    assert extracted == []


def test_extract_records_extracts_board_license_detail_for_enrichment() -> None:
    fetch = FetchResult(
        job_pk="job_board_detail",
        seed_name="NJ Psychology Board",
        seed_state="NJ",
        seed_market="statewide",
        seed_website="https://www.njconsumeraffairs.gov/psy/Pages/default.aspx",
        target_url="https://www.njconsumeraffairs.gov/psy/Applications/LicenseVerification/",
        normalized_url="https://www.njconsumeraffairs.gov/psy/Applications/LicenseVerification/",
        status_code=200,
        content=_fixture("nj_board_license_detail.html"),
        content_hash="hash_board_detail",
        fetched_at="2026-03-09T00:00:00Z",
    )
    seed = DiscoverySeed(
        name="NJ Psychology Board",
        website="https://www.njconsumeraffairs.gov/psy/Pages/default.aspx",
        state="NJ",
        market="statewide",
        tier="A",
        source_type="licensing_board",
        extraction_profile="board",
    )
    extracted = extract_records(fetch, seed, {})
    assert len(extracted) == 1
    row = extracted[0]
    assert row.provider_name == "Jane Smith"
    assert row.license_type == "psychologist"
    assert row.license_status == "active"
    assert row.diagnoses_asd == "unclear"
    assert row.diagnoses_adhd == "unclear"


def test_extract_records_builds_hospital_practice_signal_without_named_provider() -> None:
    fetch = FetchResult(
        job_pk="job_hospital",
        seed_name="RWJBarnabas Developmental Evaluations",
        seed_state="NJ",
        seed_market="Edison-New Brunswick",
        seed_website="https://www.rwjbh.org/treatment-care/pediatrics/conditions-treatments/pediatric-autism/developmental-evaluations/",
        target_url="https://www.rwjbh.org/treatment-care/pediatrics/conditions-treatments/pediatric-autism/developmental-evaluations/",
        normalized_url="https://www.rwjbh.org/treatment-care/pediatrics/conditions-treatments/pediatric-autism/developmental-evaluations/",
        status_code=200,
        content=_fixture("nj_hospital_developmental_evaluations.html"),
        content_hash="hash_hospital",
        fetched_at="2026-03-09T00:00:00Z",
    )
    seed = DiscoverySeed(
        name="RWJBarnabas Developmental Evaluations",
        website="https://www.rwjbh.org/treatment-care/pediatrics/conditions-treatments/pediatric-autism/developmental-evaluations/",
        state="NJ",
        market="Edison-New Brunswick",
        tier="A",
        source_type="hospital_directory",
        extraction_profile="hospital",
    )
    extracted = extract_records(fetch, seed, {"livingston": "Newark"})
    assert len(extracted) == 1
    row = extracted[0]
    assert row.provider_name == ""
    assert row.practice_name.startswith("Developmental Evaluations")
    assert row.diagnoses_asd == "yes"
    assert row.diagnoses_adhd == "unclear"
    assert row.phone == "(888) 724-7123"
    assert row.intake_url == "https://rwjbh.org/request-an-appointment"
    assert row.metro == "Newark"
    assert "child" in row.age_groups


def test_page_classification_marks_publication_pages_as_non_extractable() -> None:
    html = """
        <html>
        <head><title>Publications | Graduate School of Applied and Professional Psychology</title></head>
        <body>
        <h1>Publications</h1>
        <p>Lesia Ruglass, PhD authored a paper on trauma treatment.</p>
        </body>
        </html>
    """
    seed = DiscoverySeed(
        name="Rutgers Center for Adult Autism Services",
        website="https://gsapp.rutgers.edu/centers-clinics/rutgers-center-adult-autism-services-rcaas",
        state="NJ",
        market="Newark",
        tier="B",
        source_type="university_directory",
        extraction_profile="hospital",
    )
    provider_matches = _provider_candidates("Lesia Ruglass, PhD authored a paper on trauma treatment.", seed)
    classified = _classify_page(
        seed=seed,
        page_title="Publications | Graduate School of Applied and Professional Psychology",
        source_url="https://gsapp.rutgers.edu/publications",
        text="Lesia Ruglass, PhD authored a paper on trauma treatment.",
        provider_matches=provider_matches,
    )
    assert classified.role == "publication_news"
    assert classified.allow_provider_extraction is False
    assert classified.allow_practice_extraction is False



def test_extract_records_skips_publication_pages_before_provider_extraction() -> None:
    fetch = FetchResult(
        job_pk="job_publication",
        seed_name="Rutgers Center for Adult Autism Services",
        seed_state="NJ",
        seed_market="Newark",
        seed_website="https://gsapp.rutgers.edu/centers-clinics/rutgers-center-adult-autism-services-rcaas",
        target_url="https://gsapp.rutgers.edu/publications",
        normalized_url="https://gsapp.rutgers.edu/publications",
        status_code=200,
        content="""
            <html>
            <head><title>Publications | Graduate School of Applied and Professional Psychology</title></head>
            <body>
            <h1>Publications</h1>
            <p>Lesia Ruglass, PhD authored a paper on trauma treatment.</p>
            <p>Steven Sohnle, PhD discussed interventions in a webinar.</p>
            </body>
            </html>
        """,
        content_hash="hash_publication",
        fetched_at="2026-03-09T00:00:00Z",
    )
    seed = DiscoverySeed(
        name="Rutgers Center for Adult Autism Services",
        website="https://gsapp.rutgers.edu/centers-clinics/rutgers-center-adult-autism-services-rcaas",
        state="NJ",
        market="Newark",
        tier="B",
        source_type="university_directory",
        extraction_profile="hospital",
    )
    extracted = extract_records(fetch, seed, {})
    assert extracted == []



def test_extract_records_extracts_university_clinicians_conservatively() -> None:
    fetch = FetchResult(
        job_pk="job_university",
        seed_name="Rutgers Center for Adult Autism Services",
        seed_state="NJ",
        seed_market="Newark",
        seed_website="https://gsapp.rutgers.edu/centers-clinics/rutgers-center-adult-autism-services-rcaas",
        target_url="https://gsapp.rutgers.edu/centers-clinics/rutgers-center-adult-autism-services-rcaas",
        normalized_url="https://gsapp.rutgers.edu/centers-clinics/rutgers-center-adult-autism-services-rcaas",
        status_code=200,
        content=_fixture("nj_university_autism_services.html"),
        content_hash="hash_university",
        fetched_at="2026-03-09T00:00:00Z",
    )
    seed = DiscoverySeed(
        name="Rutgers Center for Adult Autism Services",
        website="https://gsapp.rutgers.edu/centers-clinics/rutgers-center-adult-autism-services-rcaas",
        state="NJ",
        market="Newark",
        tier="B",
        source_type="university_directory",
        extraction_profile="hospital",
    )
    extracted = extract_records(fetch, seed, {"new brunswick": "Edison-New Brunswick"})
    assert [row.provider_name for row in extracted] == ["Joshua Cohen", "James Maraventano"]
    assert extracted[0].diagnoses_asd == "unclear"
    assert extracted[0].diagnoses_adhd == "unclear"
    assert extracted[0].practice_name.startswith("Rutgers Center for Adult Autism Services")
    assert extracted[0].age_groups == ["adult"]
    assert extracted[1].credentials == "EdD, BCBA-D"


def main() -> None:
    test_extract_records_builds_provider_intelligence_signal()
    test_extract_records_skips_blocked_board_page()
    test_extract_records_extracts_board_license_detail_for_enrichment()
    test_extract_records_builds_hospital_practice_signal_without_named_provider()
    test_page_classification_marks_publication_pages_as_non_extractable()
    test_extract_records_skips_publication_pages_before_provider_extraction()
    test_extract_records_extracts_university_clinicians_conservatively()
    print("test_parse_stage: ok")


if __name__ == "__main__":
    main()
