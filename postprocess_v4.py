#!/usr/bin/env python3
import csv, re
from pathlib import Path
from datetime import datetime

BASE = Path('/Users/lunavanamburg/.openclaw/workspace/leads_engine')
OUT = BASE / 'out'
IN_PRIMARY = OUT / 'enriched_leads.csv'
IN_FALLBACK = OUT / 'raw_leads.csv'

# Segment rules
STORE_POSITIVE = re.compile(r'\b(dispensary|cannabis|care station|the source|nuwu|lightshade|native roots|terrapin|flowery|stiiizy|mint|rise|sunnyside|zen leaf|verilife|muv|apothecarium|botanist|jardin|medizin)\b', re.I)
NON_STORE = re.compile(r'\b(distributor|distribution|wholesale|manufacturer|cultivation|labs|holdings|ventures|industry|brands|company|corp|inc\.?|llc)\b', re.I)
JUNK_OWNER = re.compile(r'\b(with the|terms|privacy|policy|email alerts|good faith|create\s|information|respect for people|team of experts)\b', re.I)


def score_adjust(base, segment, email, phone, owner):
    s = int(base or 0)
    if segment == 'dispensary':
        s += 15
    if email: s += 5
    if phone: s += 5
    if owner: s += 5
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
    if not n: return ''
    if JUNK_OWNER.search(n): return ''
    # basic human-name heuristic: 2-3 capitalized words
    parts = n.split()
    if len(parts) < 2 or len(parts) > 3: return ''
    if not all(p[:1].isupper() for p in parts): return ''
    return n


def read_rows():
    src = IN_PRIMARY if IN_PRIMARY.exists() else IN_FALLBACK
    if not src.exists():
        return []
    return list(csv.DictReader(src.open()))


def main():
    rows = read_rows()
    now = datetime.now().isoformat(timespec='seconds')
    normalized = []
    for r in rows:
        name = r.get('dispensary') or r.get('name') or ''
        website = (r.get('website') or '').strip()
        source_url = r.get('source_url') or ''
        segment = classify_segment(name, website, source_url)
        owner = clean_owner(r.get('owner_name') or '')
        role = r.get('owner_role') or ''
        email = r.get('email') or ''
        phone = r.get('phone') or ''
        base_score = int(r.get('score') or 0)
        final_score = score_adjust(base_score, segment, email, phone, owner)
        normalized.append({
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

    normalized.sort(key=lambda x: (int(x['score']), x['dispensary']), reverse=True)

    dispensary = [x for x in normalized if x['segment'] == 'dispensary']
    non_disp = [x for x in normalized if x['segment'] != 'dispensary']

    out_all = OUT / 'v4_all_segmented.csv'
    out_disp = OUT / 'outreach_dispensary_100.csv'
    out_non = OUT / 'excluded_non_dispensary.csv'
    out_qa = OUT / 'v4_quality_report.txt'

    fields = ['dispensary','segment','website','state','market','owner_name','owner_role','email','phone','source_url','score','checked_at']

    with out_all.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(normalized)
    with out_disp.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(dispensary[:100])
    with out_non.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(non_disp)

    lines = []
    lines.append(f'V4 Quality Report ({now})')
    lines.append(f'Total rows: {len(normalized)}')
    lines.append(f'Dispensary rows: {len(dispensary)}')
    lines.append(f'Non-dispensary/unknown rows: {len(non_disp)}')
    lines.append(f'Dispensary rows w/ email: {sum(1 for r in dispensary if r["email"])}')
    lines.append(f'Dispensary rows w/ phone: {sum(1 for r in dispensary if r["phone"])}')
    lines.append(f'Dispensary rows w/ cleaned owner: {sum(1 for r in dispensary if r["owner_name"])}')
    lines.append('')
    lines.append('Top dispensary rows:')
    for r in dispensary[:15]:
        lines.append(f"- {r['dispensary']} | score {r['score']} | email={bool(r['email'])} phone={bool(r['phone'])} owner={bool(r['owner_name'])}")

    out_qa.write_text('\n'.join(lines) + '\n')
    print(f'Wrote {out_disp} ({len(dispensary[:100])} rows)')

if __name__ == '__main__':
    main()
