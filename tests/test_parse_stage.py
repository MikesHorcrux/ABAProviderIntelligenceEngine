#!/usr/bin/env python3
from __future__ import annotations

from pipeline.stages.parse import parse_page


def test_parse_page_extracts_contact_and_menu_signals():
    html = """
        <html>
        <body>
        <a href="/about">About us</a>
        <a href="/team">Team</a>
        <p>Contact us at owner@greenleaf.com or (415) 555-0102</p>
        <p>Owner Jane Smith handles operations</p>
        <script>const p="https://www.dutchie.com/menudemo"</script>
        <a href="https://www.instagram.com/greenleaf">Instagram</a>
        <a href="/careers">Careers</a>
        </body>
        </html>
    """
    parsed = parse_page("https://greenleaf.com", html)
    assert parsed.menu_providers == ["dutchie"]
    assert any(s.value == "owner@greenleaf.com" for s in parsed.emails)
    assert any(s.value == "(415) 555-0102" for s in parsed.phones)
    assert any(name == "Jane Smith" for name, _, _ in parsed.contact_people)
    assert any("instagram" in u for u in parsed.social_urls)
    assert parsed.schema_local_business == {}


def main() -> None:
    test_parse_page_extracts_contact_and_menu_signals()
    print("test_parse_stage: ok")


if __name__ == "__main__":
    main()
