from __future__ import annotations

import csv
import html
import json
import sqlite3
import textwrap
from pathlib import Path


def _safe_slug(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-")
    return cleaned or "record"


def _bundle(con: sqlite3.Connection, record_id: str) -> dict[str, object]:
    record = con.execute(
        """
        SELECT pr.*, pl.city, pl.state AS location_state, pl.metro, pl.phone AS location_phone,
               p.provider_name, p.credentials, pt.website, pt.intake_url, pr.practice_name_snapshot AS practice_name
        FROM provider_practice_records pr
        INNER JOIN providers p ON p.provider_id = pr.provider_id
        INNER JOIN practices pt ON pt.practice_id = pr.practice_id
        INNER JOIN practice_locations pl ON pl.location_id = pr.location_id
        WHERE pr.record_id=?
        """,
        (record_id,),
    ).fetchone()
    evidence = [dict(row) for row in con.execute("SELECT * FROM field_evidence WHERE record_id=? ORDER BY field_name, captured_at", (record_id,))]
    contradictions = [dict(row) for row in con.execute("SELECT * FROM contradictions WHERE record_id=? ORDER BY field_name", (record_id,))]
    return {
        "record": dict(record) if record else {},
        "evidence": evidence,
        "contradictions": contradictions,
    }


def _diagnostic_focus(record: dict[str, object]) -> str:
    asd = str(record.get("diagnoses_asd") or "unclear")
    adhd = str(record.get("diagnoses_adhd") or "unclear")
    if asd == "yes" and adhd == "yes":
        return "ASD and ADHD diagnostic services"
    if asd == "yes":
        return "autism diagnostic services"
    if adhd == "yes":
        return "ADHD diagnostic services"
    return "developmental and behavioral evaluation services"


def _target_buyer(record: dict[str, object]) -> str:
    credentials = str(record.get("credentials") or "").lower()
    if "md" in credentials or "do" in credentials:
        return "medical director or practice owner"
    if "psyd" in credentials or "phd" in credentials:
        return "clinical director or practice owner"
    if "apn" in credentials or "np" in credentials or "pa" in credentials:
        return "clinical lead or practice administrator"
    return "practice administrator or intake lead"


def _outreach_angle(record: dict[str, object]) -> str:
    focus = _diagnostic_focus(record)
    telehealth = str(record.get("telehealth") or "unknown")
    referral = str(record.get("referral_requirements") or "").strip() or "unknown"
    if telehealth == "yes":
        return f"Lead with referral capture and intake efficiency for {focus}, including telehealth-enabled scheduling capacity."
    if referral != "unknown":
        return f"Lead with faster referral routing and intake conversion for {focus}, since the practice already publishes referral requirements."
    return f"Lead with diagnostic demand capture, intake conversion, and referral visibility for {focus}."


def _outreach_opener(record: dict[str, object]) -> str:
    practice = str(record.get("practice_name") or record.get("practice_name_snapshot") or "").strip()
    city = str(record.get("city") or "").strip()
    focus = _diagnostic_focus(record)
    locale = f" in {city}" if city else ""
    return f"{practice}{locale} publicly advertises {focus}; open with referral demand, intake throughput, and evaluation booking friction."


def _evidence_summary(bundle: dict[str, object]) -> str:
    evidence = list(bundle.get("evidence") or [])
    preferred_fields = ("diagnoses_asd", "diagnoses_adhd", "license_status", "prescriptive_authority")
    snippets: list[str] = []
    for field in preferred_fields:
        item = next((entry for entry in evidence if str(entry.get("field_name") or entry.get("field") or "") == field), None)
        if not item:
            continue
        quote = str(item.get("quote") or "").strip()
        if quote:
            snippets.append(quote)
    return " | ".join(snippets[:3])


def _sales_bundle_row(bundle: dict[str, object]) -> dict[str, object]:
    record = dict(bundle.get("record") or {})
    evidence = list(bundle.get("evidence") or [])
    source_urls = sorted({str(item.get("source_url") or "") for item in evidence if str(item.get("source_url") or "")})
    return {
        "record_id": record.get("record_id", ""),
        "provider_name": record.get("provider_name", ""),
        "credentials": record.get("credentials", ""),
        "practice_name": record.get("practice_name", ""),
        "city": record.get("city", ""),
        "state": record.get("location_state", ""),
        "metro": record.get("metro", ""),
        "phone": record.get("location_phone", ""),
        "website": record.get("website", ""),
        "intake_url": record.get("intake_url", ""),
        "diagnoses_asd": record.get("diagnoses_asd", "unclear"),
        "diagnoses_adhd": record.get("diagnoses_adhd", "unclear"),
        "license_status": record.get("license_status", "unknown"),
        "prescriptive_authority": record.get("prescriptive_authority", "unknown"),
        "record_confidence": record.get("record_confidence", 0.0),
        "outreach_fit_score": record.get("outreach_fit_score", 0.0),
        "target_buyer": _target_buyer(record),
        "outreach_angle": _outreach_angle(record),
        "opener": _outreach_opener(record),
        "evidence_summary": _evidence_summary(bundle),
        "source_urls": source_urls,
    }


def _sales_markdown(bundle: dict[str, object]) -> str:
    row = _sales_bundle_row(bundle)
    lines = [
        f"# Sales Brief - {row['provider_name']} / {row['practice_name']}",
        "",
        "## Target",
        f"- Buyer: {row['target_buyer']}",
        f"- Phone: {row['phone'] or 'unknown'}",
        f"- Website: {row['website'] or 'unknown'}",
        f"- Intake URL: {row['intake_url'] or 'unknown'}",
        "",
        "## Why This Record Matters",
        f"- Diagnostic focus: {_diagnostic_focus(dict(bundle.get('record') or {}))}",
        f"- License status: {row['license_status']}",
        f"- Prescribing capability: {row['prescriptive_authority']}",
        f"- Record confidence: {row['record_confidence']}",
        f"- Outreach fit score: {row['outreach_fit_score']}",
        "",
        "## Recommended Angle",
        f"- {row['outreach_angle']}",
        "",
        "## Suggested Opener",
        f"- {row['opener']}",
        "",
        "## Evidence Summary",
        f"- {row['evidence_summary'] or 'See cited evidence bundle.'}",
        "",
        "## Evidence Links",
    ]
    for url in row["source_urls"]:
        lines.append(f"- {url}")
    if not row["source_urls"]:
        lines.append("- No evidence links captured.")
    return "\n".join(lines).strip() + "\n"


def _markdown_profile(bundle: dict[str, object]) -> str:
    record = dict(bundle.get("record") or {})
    evidence = list(bundle.get("evidence") or [])
    contradictions = list(bundle.get("contradictions") or [])
    source_urls = sorted({str(item.get("source_url") or "") for item in evidence if str(item.get("source_url") or "")})
    caveats = record.get("blocked_reason") or record.get("conflict_note") or "No major caveats."
    lines = [
        f"# {record.get('provider_name', '')} - {record.get('practice_name', '')}",
        "",
        "## Summary",
        f"- Credentials: {record.get('credentials', 'unknown')}",
        f"- License: {record.get('license_type', 'unknown')} / {record.get('license_status', 'unknown')} / {record.get('license_state', 'unknown')}",
        f"- Record confidence: {record.get('record_confidence', 0.0)}",
        f"- Last verified: {record.get('last_verified_at', '')}",
        "",
        "## Diagnostic capability",
        f"- ASD diagnosis: {record.get('diagnoses_asd', 'unclear')}",
        f"- ADHD diagnosis: {record.get('diagnoses_adhd', 'unclear')}",
        "",
        "## Prescribing capability",
        f"- Authority: {record.get('prescriptive_authority', 'unknown')}",
        f"- Basis: {record.get('prescriptive_basis', '')}",
        "",
        "## Practice details",
        f"- Practice: {record.get('practice_name', '')}",
        f"- City/state: {record.get('city', '')}, {record.get('location_state', '')}",
        f"- Metro: {record.get('metro', '')}",
        f"- Phone: {record.get('location_phone', '')}",
        f"- Telehealth: {record.get('telehealth', 'unknown')}",
        f"- Age groups: {record.get('age_groups_json', '[]')}",
        f"- Insurance notes: {record.get('insurance_notes', '') or 'unknown'}",
        f"- Waitlist notes: {record.get('waitlist_notes', '') or 'unknown'}",
        f"- Referral requirements: {record.get('referral_requirements', '') or 'unknown'}",
        "",
        "## Evidence links",
    ]
    for url in source_urls:
        lines.append(f"- {url}")
    if not source_urls:
        lines.append("- No evidence links captured.")
    lines.extend(["", "## Confidence and caveats", f"- Caveats: {caveats}"])
    if contradictions:
        lines.append(f"- Contradictions: {len(contradictions)} source conflicts captured.")
    return "\n".join(lines).strip() + "\n"


def _markdown_to_html(markdown: str) -> str:
    html_lines = [
        "<html><head><meta charset='utf-8'><style>",
        "body{font-family:Helvetica,Arial,sans-serif;margin:40px;color:#1f2933;}",
        "h1,h2{color:#0f172a;} ul{padding-left:20px;} li{margin:4px 0;} p{line-height:1.5;}",
        "</style></head><body>",
    ]
    in_list = False
    for raw in markdown.splitlines():
        line = raw.strip()
        if not line:
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            continue
        if line.startswith("# "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h1>{html.escape(line[2:])}</h1>")
            continue
        if line.startswith("## "):
            if in_list:
                html_lines.append("</ul>")
                in_list = False
            html_lines.append(f"<h2>{html.escape(line[3:])}</h2>")
            continue
        if line.startswith("- "):
            if not in_list:
                html_lines.append("<ul>")
                in_list = True
            html_lines.append(f"<li>{html.escape(line[2:])}</li>")
            continue
        if in_list:
            html_lines.append("</ul>")
            in_list = False
        html_lines.append(f"<p>{html.escape(line)}</p>")
    if in_list:
        html_lines.append("</ul>")
    html_lines.append("</body></html>")
    return "\n".join(html_lines)


def _pdf_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _fallback_pdf_bytes(markdown: str) -> bytes:
    lines = []
    for raw in markdown.splitlines():
        if not raw.strip():
            lines.append("")
            continue
        lines.extend(textwrap.wrap(raw, width=88) or [""])
    content_lines = ["BT", "/F1 10 Tf", "50 760 Td", "14 TL"]
    for line in lines[:48]:
        content_lines.append(f"({_pdf_escape(line)}) Tj")
        content_lines.append("T*")
    content_lines.append("ET")
    stream = "\n".join(content_lines).encode("latin-1", errors="ignore")

    objects = [
        b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n",
        b"2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n",
        b"3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj\n",
        f"4 0 obj << /Length {len(stream)} >> stream\n".encode("latin-1") + stream + b"\nendstream endobj\n",
        b"5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj\n",
    ]

    output = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for obj in objects:
        offsets.append(len(output))
        output.extend(obj)
    xref_start = len(output)
    output.extend(f"xref\n0 {len(offsets)}\n".encode("latin-1"))
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode("latin-1"))
    output.extend(
        f"trailer << /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n".encode("latin-1")
    )
    return bytes(output)


def _write_pdf(markdown: str, pdf_path: Path) -> None:
    pdf_path.write_bytes(_fallback_pdf_bytes(markdown))


def export_provider_intel(con: sqlite3.Connection, out_dir: Path, run_id: str, limit: int = 100) -> dict[str, object]:
    root = out_dir / "provider_intel"
    root.mkdir(parents=True, exist_ok=True)
    records_path = root / f"provider_records_{run_id}.csv"
    json_path = root / f"provider_records_{run_id}.json"
    review_path = root / f"review_queue_{run_id}.csv"
    sales_path = root / f"sales_report_{run_id}.csv"
    profiles_dir = root / "profiles"
    evidence_dir = root / "evidence"
    outreach_dir = root / "outreach"
    profiles_dir.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    outreach_dir.mkdir(parents=True, exist_ok=True)

    approved_rows = con.execute(
        """
        SELECT pr.record_id, p.provider_name, p.credentials, p.npi, pr.license_state, pr.license_type, pr.license_status,
               pr.practice_name_snapshot AS practice_name, pl.city, pl.state, pl.metro, pl.phone, pt.website, pt.intake_url,
               pr.diagnoses_asd, pr.diagnoses_adhd, pr.prescriptive_authority, pr.prescriptive_basis, pr.age_groups_json,
               pr.telehealth, pr.insurance_notes, pr.waitlist_notes, pr.referral_requirements, pr.source_urls_json,
               pr.field_confidence_json, pr.record_confidence, pr.outreach_fit_score, pr.outreach_ready, pr.outreach_reasons_json,
               pr.last_verified_at
        FROM provider_practice_records pr
        INNER JOIN providers p ON p.provider_id = pr.provider_id
        INNER JOIN practices pt ON pt.practice_id = pr.practice_id
        INNER JOIN practice_locations pl ON pl.location_id = pr.location_id
        WHERE pr.export_status='approved'
        ORDER BY pr.outreach_fit_score DESC, pr.record_confidence DESC, p.provider_name ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    fieldnames = [
        "provider_id",
        "provider_name",
        "credentials",
        "npi",
        "license_state",
        "license_type",
        "license_status",
        "practice_name",
        "city",
        "state",
        "metro",
        "phone",
        "website",
        "intake_url",
        "diagnoses_asd",
        "diagnoses_adhd",
        "prescriptive_authority",
        "prescriptive_basis",
        "age_groups",
        "telehealth",
        "insurance_notes",
        "waitlist_notes",
        "referral_requirements",
        "source_urls",
        "field_confidence",
        "record_confidence",
        "outreach_fit_score",
        "outreach_ready",
        "outreach_reasons",
        "last_verified_at",
    ]
    export_rows: list[dict[str, object]] = []
    for row in approved_rows:
        source_urls = json.loads(row["source_urls_json"] or "[]")
        field_confidence = json.loads(row["field_confidence_json"] or "{}")
        outreach_reasons = json.loads(row["outreach_reasons_json"] or "[]")
        export_rows.append(
            {
                "provider_id": row["record_id"],
                "provider_name": row["provider_name"],
                "credentials": row["credentials"],
                "npi": row["npi"],
                "license_state": row["license_state"],
                "license_type": row["license_type"],
                "license_status": row["license_status"],
                "practice_name": row["practice_name"],
                "city": row["city"],
                "state": row["state"],
                "metro": row["metro"],
                "phone": row["phone"],
                "website": row["website"],
                "intake_url": row["intake_url"],
                "diagnoses_asd": row["diagnoses_asd"],
                "diagnoses_adhd": row["diagnoses_adhd"],
                "prescriptive_authority": row["prescriptive_authority"],
                "prescriptive_basis": row["prescriptive_basis"],
                "age_groups": json.loads(row["age_groups_json"] or "[]"),
                "telehealth": row["telehealth"],
                "insurance_notes": row["insurance_notes"] or "unknown",
                "waitlist_notes": row["waitlist_notes"] or "unknown",
                "referral_requirements": row["referral_requirements"] or "unknown",
                "source_urls": source_urls,
                "field_confidence": field_confidence,
                "record_confidence": row["record_confidence"],
                "outreach_fit_score": row["outreach_fit_score"],
                "outreach_ready": int(row["outreach_ready"] or 0),
                "outreach_reasons": outreach_reasons,
                "last_verified_at": row["last_verified_at"],
            }
        )

    with records_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in export_rows:
            flattened = dict(row)
            flattened["age_groups"] = json.dumps(flattened["age_groups"])
            flattened["source_urls"] = json.dumps(flattened["source_urls"])
            flattened["field_confidence"] = json.dumps(flattened["field_confidence"], sort_keys=True)
            flattened["outreach_reasons"] = json.dumps(flattened["outreach_reasons"], sort_keys=True)
            writer.writerow(flattened)

    json_path.write_text(json.dumps(export_rows, indent=2, default=str), encoding="utf-8")

    sales_rows: list[dict[str, object]] = []
    for row in approved_rows:
        bundle = _bundle(con, row["record_id"])
        slug = _safe_slug(f"{row['provider_name']}-{row['practice_name']}")
        record_dir = profiles_dir / f"{row['record_id']}-{slug}"
        record_dir.mkdir(parents=True, exist_ok=True)
        evidence_path = evidence_dir / f"{row['record_id']}.json"
        markdown_path = record_dir / "profile.md"
        pdf_path = record_dir / "profile.pdf"
        markdown = _markdown_profile(bundle)
        markdown_path.write_text(markdown, encoding="utf-8")
        evidence_path.write_text(json.dumps(bundle, indent=2, default=str), encoding="utf-8")
        _write_pdf(markdown, pdf_path)
        if int(row["outreach_ready"] or 0):
            sales_row = _sales_bundle_row(bundle)
            sales_rows.append(sales_row)
            outreach_record_dir = outreach_dir / f"{row['record_id']}-{slug}"
            outreach_record_dir.mkdir(parents=True, exist_ok=True)
            sales_markdown_path = outreach_record_dir / "sales_brief.md"
            sales_pdf_path = outreach_record_dir / "sales_brief.pdf"
            sales_markdown = _sales_markdown(bundle)
            sales_markdown_path.write_text(sales_markdown, encoding="utf-8")
            _write_pdf(sales_markdown, sales_pdf_path)

    sales_fieldnames = [
        "record_id",
        "provider_name",
        "credentials",
        "practice_name",
        "city",
        "state",
        "metro",
        "phone",
        "website",
        "intake_url",
        "diagnoses_asd",
        "diagnoses_adhd",
        "license_status",
        "prescriptive_authority",
        "record_confidence",
        "outreach_fit_score",
        "target_buyer",
        "outreach_angle",
        "opener",
        "evidence_summary",
        "source_urls",
    ]
    with sales_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=sales_fieldnames)
        writer.writeheader()
        for row in sales_rows:
            flattened = dict(row)
            flattened["source_urls"] = json.dumps(flattened["source_urls"])
            writer.writerow(flattened)

    review_rows = [
        dict(row)
        for row in con.execute(
            """
            SELECT review_id, record_id, review_type, provider_name, practice_name, reason, source_url, evidence_quote, status, created_at
            FROM review_queue
            ORDER BY created_at DESC, review_id ASC
            """
        )
    ]
    with review_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["review_id", "record_id", "review_type", "provider_name", "practice_name", "reason", "source_url", "evidence_quote", "status", "created_at"],
        )
        writer.writeheader()
        writer.writerows(review_rows)

    return {
        "records_csv": str(records_path),
        "records_json": str(json_path),
        "review_queue_csv": str(review_path),
        "sales_report_csv": str(sales_path),
        "profiles_dir": str(profiles_dir),
        "evidence_dir": str(evidence_dir),
        "outreach_dir": str(outreach_dir),
        "record_count": len(export_rows),
        "sales_count": len(sales_rows),
        "review_count": len(review_rows),
    }
