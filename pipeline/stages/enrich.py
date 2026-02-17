from __future__ import annotations

import re

from pipeline.utils import make_pk, utcnow_iso, is_valid_email


EMAIL_PREFIX_PATTERNS = re.compile(r"[^a-zA-Z]")


def _infer_emails_from_person(location_name: str, domain: str) -> list[str]:
    if not location_name or not domain:
        return []
    parts = re.findall(r"[A-Za-z]+", location_name)
    if len(parts) < 2:
        return []
    first = parts[0].lower()
    last = parts[-1].lower()
    return [
        f"{first}.{last}@{domain}",
        f"{first}{last}@{domain}",
        f"{first[0]}.{last}@{domain}",
    ]


def run_waterfall_enrichment(con, location_pk: str) -> None:
    now = utcnow_iso()
    rows = con.execute(
        """
        SELECT cp.value, cp.type
        FROM contact_points cp
        WHERE cp.location_pk = ?
          AND cp.value <> ''
        """,
        (location_pk,),
    ).fetchall()

    emails = [r["value"] for r in rows if r["type"] == "email" and is_valid_email(r["value"])]
    phone_row = con.execute(
        "SELECT value FROM contact_points WHERE location_pk=? AND type='phone' LIMIT 1",
        (location_pk,),
    ).fetchone()
    person = con.execute(
        "SELECT full_name, role FROM contacts WHERE location_pk=? AND deleted_at IS NULL ORDER BY confidence DESC, updated_at DESC LIMIT 1",
        (location_pk,),
    ).fetchone()

    domain_row = con.execute("SELECT domain FROM domains WHERE location_pk=? LIMIT 1", (location_pk,)).fetchone()
    domain = domain_row["domain"] if domain_row else ""
    role_person = person["role"] if person else ""

    if not emails and domain and person and person["full_name"]:
        for candidate in _infer_emails_from_person(person["full_name"], domain):
            if not is_valid_email(candidate):
                continue
            # Skip if this inferred address already exists as a direct contact point.
            if con.execute(
                "SELECT 1 FROM contact_points WHERE location_pk=? AND type='email' AND value=? LIMIT 1",
                (location_pk, candidate),
            ).fetchone():
                continue

            con.execute(
                """
                INSERT OR REPLACE INTO contacts
                (contact_pk, location_pk, full_name, role, email, phone, source_kind, confidence, verification_status, created_at, updated_at, last_seen_at, deleted_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'')
                """,
                (
                    make_pk("c", [location_pk, "inferred", candidate]),
                    location_pk,
                    person["full_name"],
                    role_person or "owner",
                    candidate,
                    phone_row["value"] if phone_row else "",
                    "email_inference",
                    0.21,
                    "unverified",
                    now,
                    now,
                    now,
                ),
            )
            con.execute(
                """
                INSERT OR REPLACE INTO evidence
                (evidence_pk, entity_type, entity_pk, field_name, field_value, source_url, snippet, captured_at, deleted_at)
                VALUES (?,?,?,?,?,?,?,?,'')
                """,
                (
                    make_pk("ev", [location_pk, "inferred_email", candidate]),
                    "location",
                    location_pk,
                    "inferred_email",
                    candidate,
                    "",
                    "inferred from contact name + domain",
                    now,
                ),
            )
            break

    # Lightweight verification signal only (syntax check already enforced above).
    con.execute(
        "UPDATE contacts SET verification_status='unverified' WHERE location_pk=? AND email<>''",
        (location_pk,),
    )
    con.execute(
        "UPDATE locations SET fit_score = COALESCE(fit_score, 0), updated_at=?, last_seen_at=? WHERE location_pk=?",
        (now, now, location_pk),
    )
    con.commit()
