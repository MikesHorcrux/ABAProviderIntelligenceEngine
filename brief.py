#!/usr/bin/env python3
import csv
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parent
PRIMARY = BASE / 'out/outreach_dispensary_100.csv'
SECONDARY = BASE / 'out/enriched_leads.csv'
FALLBACK = BASE / 'out/verified_leads.csv'
OUT = BASE / 'out/morning_brief.txt'

rows = []
if PRIMARY.exists():
    src = PRIMARY
elif SECONDARY.exists():
    src = SECONDARY
else:
    src = FALLBACK
if src.exists():
    with open(src, newline='') as f:
        rows = list(csv.DictReader(f))

high = [r for r in rows if (r.get('owner_confidence') or '').lower() == 'high']
with_email = [r for r in rows if r.get('email')]
with_owner = [r for r in rows if r.get('owner_name')]

top = rows[:10]
now = datetime.now().strftime('%Y-%m-%d %I:%M %p')

lines = []
lines.append(f"Morning Brief ({now})")
lines.append('')
lines.append(f"Total leads in database: {len(rows)}")
lines.append(f"Leads with owner identified: {len(with_owner)}")
lines.append(f"High-confidence owner leads: {len(high)}")
lines.append(f"Leads with email found: {len(with_email)}")
lines.append('')
lines.append('Top 10 leads by score:')
for i, r in enumerate(top, 1):
    lines.append(f"{i}. {r['dispensary']} ({r['state']}) — score {r['score']} — owner: {r.get('owner_name') or 'n/a'} {('('+r.get('owner_role')+')') if r.get('owner_role') else ''}")

OUT.write_text('\n'.join(lines) + '\n')
print(str(OUT))
