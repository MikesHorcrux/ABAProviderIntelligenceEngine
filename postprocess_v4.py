#!/usr/bin/env python3
import csv
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

BASE = Path(__file__).resolve().parent
OUT = BASE / 'out'
IN_PRIMARY = OUT / 'enriched_leads.csv'
IN_FALLBACK = OUT / 'raw_leads.csv'
CANONICAL_DB = BASE / 'data/cannaradar_v1.db'

# Segment rules
STORE_POSITIVE = re.compile(r'\b(dispensary|cannabis|care station|the source|nuwu|lightshade|native roots|terrapin|flowery|stiiizy|mint|rise|sunnyside|zen leaf|verilife|muv|apothecarium|botanist|jardin|medizin)\b', re.I)
NON_STORE = re.compile(r'\b(distributor|distribution|wholesale|manufacturer|cultivation|labs|holdings|ventures|industry|brands|company|corp|inc\.?|llc)\b', re.I)
JUNK_OWNER = re.compile(r'\b(with the|terms|privacy|policy|email alerts|good faith|create\s|information|respect for people|team of experts)\b', re.I)

PIPELINE_FIELDS = {'dispensary', 'website', 'state', 'market', 'owner_name', 'owner_role', 'email', 'phone', 'source_url', 'score', 'checked_at'}
CANONICAL_FIELDS = {'dispensary', 'website', 'state', 'market', 'email', 'phone', 'source_url', 'score', 'checked_at'}


def normalize_website(url: str) -> str:
  if not url:
    return ''
  v = url.strip().lower()
  if not v:
    return ''
  if '://' not in v:
    v = 'https://' + v
  p = urlparse(v)
  host = (p.netloc or '').lower()
  if host.startswith('www.'):
    host = host[4:]
  if host and p.path:
    path = p.path.rstrip('/')
    if path and path != '/':
      return f'{host}{path}'
  return host


def to_int(v, default=0) -> int:
  try:
    return int(str(v).strip())
  except Exception:
    return default


def score_adjust(base, segment, email, phone, owner):
    s = int(to_int(base, 0))
    if segment == 'dispensary':
        s += 15
    if email:
        s += 5
    if phone:
        s += 5
    if owner:
        s += 5
    return max(0, min(100, s))


def classify_segment(name, website, source_url):
    txt = ' '.join([name or '', website or '', source_url or ''])
    if NON_STORE.search(txt) and not STORE_POSITIVE.search(txt):
        return 'non-dispensary'
    if STORE_POSITIVE.search(txt):
        return 'dispensary'
    return 'unknown'


def clean_owner(name):
    n = (name or '').strip()
    if not n:
        return ''
    if JUNK_OWNER.search(n):
        return ''
    parts = n.split()
    if len(parts) < 2 or len(parts) > 3:
        return ''
    if not all(p[:1].isupper() for p in parts):
        return ''
    return n


def read_pipeline_rows():
    src = IN_PRIMARY if IN_PRIMARY.exists() else IN_FALLBACK
    if not src.exists():
        return []
    return list(csv.DictReader(src.open()))


def read_canonical_rows():
    if not CANONICAL_DB.exists():
        return []

    con = sqlite3.connect(CANONICAL_DB)
    rows = con.execute(
        '''SELECT l.canonical_name, l.website_domain, l.state, l.phone,
                  MAX(CASE WHEN cp.type='email' THEN cp.value ELSE '' END) as email,
                  MAX(CASE WHEN cp.type='website' THEN cp.value ELSE '' END) as source_url
           FROM locations l
           LEFT JOIN contact_points cp ON cp.location_pk = l.location_pk
           GROUP BY l.location_pk, l.canonical_name, l.website_domain, l.state, l.phone'''
    ).fetchall()
    con.close()

    out = []
    for name, website, state, phone, email, source_url in rows:
        out.append({
            'dispensary': name or '',
            'website': website or '',
            'state': state or '',
            'market': '',
            'owner_name': '',
            'owner_role': '',
            'email': email or '',
            'phone': phone or '',
            'source_url': source_url or website or '',
            'score': '0',
            'checked_at': '',
        })
    return out


def row_key(row):
    website = normalize_website(row.get('website') or row.get('source_url'))
    name = (row.get('dispensary') or row.get('name') or '').strip().lower()
    state = (row.get('state') or '').strip().lower()
    return (website if website else name, state)


def dedupe_rows(rows):
    out = {}
    for r in rows:
        key = row_key(r)
        if not any(key):
            continue
        current = out.get(key)
        if current is None:
            out[key] = r
            continue
        if to_int(r.get('score', 0)) > to_int(current.get('score', 0)):
            out[key] = r
    return list(out.values())


def merge_rows(pipeline_row, canonical_row):
    merged = {k: '' for k in PIPELINE_FIELDS | CANONICAL_FIELDS}
    for k in merged:
        pv = (pipeline_row or {}).get(k, '')
        cv = (canonical_row or {}).get(k, '')
        merged[k] = (pv or cv or '')
    merged['source_url'] = (pipeline_row or {}).get('source_url', '').strip() or (canonical_row or {}).get('source_url', '').strip()
    merged['checked_at'] = (pipeline_row or {}).get('checked_at', '').strip() or (canonical_row or {}).get('checked_at', '').strip()
    return merged


def main():
    now = datetime.now().isoformat(timespec='seconds')
    pipeline_rows = dedupe_rows(read_pipeline_rows())
    canonical_rows = dedupe_rows(read_canonical_rows())

    canonical_map = {row_key(r): r for r in canonical_rows}
    used_keys = set()
    normalized = []

    for r in pipeline_rows:
        key = row_key(r)
        used_keys.add(key)
        merged = merge_rows(r, canonical_map.get(key, {}))
        normalized.append(merged)

    for r in canonical_rows:
        key = row_key(r)
        if key in used_keys:
            continue
        normalized.append(merge_rows({}, r))

    final_rows = []
    for r in normalized:
        name = r.get('dispensary') or r.get('name') or ''
        website = (r.get('website') or '').strip()
        source_url = r.get('source_url') or ''
        segment = classify_segment(name, website, source_url)
        owner = clean_owner(r.get('owner_name') or '')
        role = r.get('owner_role') or ''
        email = r.get('email') or ''
        phone = r.get('phone') or ''
        base_score = to_int(r.get('score'), 0)
        final_score = score_adjust(base_score, segment, email, phone, owner)
        final_rows.append({
            'dispensary': name,
            'segment': segment,
            'website': website,
            'state': r.get('state') or '',
            'market': r.get('market') or '',
            'owner_name': owner,
            'owner_role': role,
            'email': email,
            'phone': phone,
            'source_url': source_url,
            'score': str(final_score),
            'checked_at': r.get('checked_at') or now,
        })

    final_rows.sort(key=lambda x: (int(x['score']), x['dispensary']), reverse=True)

    dispensary = [x for x in final_rows if x['segment'] == 'dispensary']
    non_disp = [x for x in final_rows if x['segment'] != 'dispensary']

    out_all = OUT / 'v4_all_segmented.csv'
    out_disp = OUT / 'outreach_dispensary_100.csv'
    out_non = OUT / 'excluded_non_dispensary.csv'
    out_qa = OUT / 'v4_quality_report.txt'

    fields = ['dispensary','segment','website','state','market','owner_name','owner_role','email','phone','source_url','score','checked_at']

    OUT.mkdir(parents=True, exist_ok=True)
    with out_all.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(final_rows)
    with out_disp.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(dispensary[:100])
    with out_non.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(non_disp)

    lines = []
    lines.append(f'V4 Quality Report ({now})')
    lines.append(f'Total rows: {len(final_rows)}')
    lines.append(f'Dispensary rows: {len(dispensary)}')
    lines.append(f'Non-dispensary/unknown rows: {len(non_disp)}')
    lines.append(f'Dispensary rows w/ email: {sum(1 for r in dispensary if r["email"])}')
    lines.append(f'Dispensary rows w/ phone: {sum(1 for r in dispensary if r["phone"])}')
    lines.append(f'Dispensary rows w/ cleaned owner: {sum(1 for r in dispensary if r["owner_name"])}')
    lines.append(f'Canonical rows merged: {len(canonical_rows)}')
    lines.append(f'Pipeline rows used: {len(pipeline_rows)}')
    lines.append('')
    lines.append('Top dispensary rows:')
    for r in dispensary[:15]:
        lines.append(f"- {r['dispensary']} | score {r['score']} | email={bool(r['email'])} phone={bool(r['phone'])} owner={bool(r['owner_name'])}")

    out_qa.write_text('\n'.join(lines) + '\n')
    print(f'Wrote {out_disp} ({len(dispensary[:100])} rows)')


if __name__ == '__main__':
    main()
