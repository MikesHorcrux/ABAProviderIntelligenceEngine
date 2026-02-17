#!/usr/bin/env python3
import csv, re, sqlite3, time, socket
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
from pathlib import Path
from datetime import datetime

BASE = Path(__file__).resolve().parent
SEEDS = BASE / 'seeds.csv'
DB = BASE / 'data/leads.db'
OUT = BASE / 'out/verified_leads.csv'
UA = 'Mozilla/5.0 (compatible; LunaLeadCrawler/1.0; +local)'
socket.setdefaulttimeout(2)

EMAIL_RE = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
PHONE_RE = re.compile(r'(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')
ROLE_RE = re.compile(r'\b(Founder|Co[- ]?Founder|Owner|CEO|President|Chief Executive Officer|COO|VP Operations|Director of Operations)\b', re.I)
NAME_NEAR_ROLE_RE = re.compile(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}).{0,60}\b(Founder|Owner|CEO|President|COO)\b', re.I|re.S)

PAGES = ['', '/contact']


def fetch(url, timeout=2):
    try:
        req = Request(url, headers={'User-Agent': UA})
        with urlopen(req, timeout=timeout) as r:
            ct = r.headers.get('Content-Type','')
            if 'text/html' not in ct:
                return ''
            return r.read().decode('utf-8', errors='ignore')
    except Exception:
        return ''


def strip_html(s):
    s = re.sub(r'<script[\s\S]*?</script>', ' ', s, flags=re.I)
    s = re.sub(r'<style[\s\S]*?</style>', ' ', s, flags=re.I)
    s = re.sub(r'<[^>]+>', ' ', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()


def score(owner, role, email, phone):
    sc = 0
    if owner: sc += 45
    if role: sc += 25
    if email: sc += 20
    if phone: sc += 10
    return min(sc, 100)


def init_db():
    DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB)
    con.execute('''
    CREATE TABLE IF NOT EXISTS leads (
      id INTEGER PRIMARY KEY,
      name TEXT,
      website TEXT,
      state TEXT,
      market TEXT,
      owner_name TEXT,
      owner_role TEXT,
      owner_confidence TEXT,
      email TEXT,
      phone TEXT,
      source_url TEXT,
      fit_reason TEXT,
      score INTEGER,
      checked_at TEXT
    )''')
    con.commit()
    return con


def upsert(con, row):
    con.execute('DELETE FROM leads WHERE website=?', (row['website'],))
    con.execute('''INSERT INTO leads
      (name,website,state,market,owner_name,owner_role,owner_confidence,email,phone,source_url,fit_reason,score,checked_at)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
      (row['name'], row['website'], row['state'], row['market'], row['owner_name'], row['owner_role'], row['owner_confidence'], row['email'], row['phone'], row['source_url'], row['fit_reason'], row['score'], row['checked_at']))
    con.commit()


def main():
    con = init_db()
    now = datetime.now().isoformat(timespec='seconds')
    with open(SEEDS, newline='') as f:
        seeds = list(csv.DictReader(f))

    for s in seeds:
        base = s['website'].rstrip('/')
        print(f"Checking {s['name']} ({base})", flush=True)
        best = {'owner_name':'', 'owner_role':'', 'email':'', 'phone':'', 'source_url':base}
        for p in PAGES:
            u = base + p
            html = fetch(u)
            if not html:
                continue
            txt = strip_html(html)
            emails = [e for e in EMAIL_RE.findall(txt) if not e.lower().endswith(('.png','.jpg','.jpeg','.webp','.gif'))]
            phones = PHONE_RE.findall(txt)
            m = NAME_NEAR_ROLE_RE.search(txt)
            role_m = ROLE_RE.search(txt)
            owner = m.group(1).strip() if m else ''
            role = m.group(2).strip() if m else (role_m.group(1) if role_m else '')
            # choose best by completeness
            cand = {
                'owner_name': owner[:120],
                'owner_role': role[:80],
                'email': (emails[0] if emails else '')[:160],
                'phone': (phones[0] if phones else '')[:40],
                'source_url': u,
            }
            if score(cand['owner_name'], cand['owner_role'], cand['email'], cand['phone']) > score(best['owner_name'], best['owner_role'], best['email'], best['phone']):
                best = cand
            time.sleep(0.15)

        sc = score(best['owner_name'], best['owner_role'], best['email'], best['phone'])
        conf = 'High' if sc >= 75 else ('Medium' if sc >= 45 else 'Low')
        fit = 'Inventory-heavy dispensary operations; fit for AI stock monitoring, replenishment alerts, and variance reduction.'
        row = {
            'name': s['name'], 'website': s['website'], 'state': s['state'], 'market': s['market'],
            'owner_name': best['owner_name'], 'owner_role': best['owner_role'], 'owner_confidence': conf,
            'email': best['email'], 'phone': best['phone'], 'source_url': best['source_url'],
            'fit_reason': fit, 'score': sc, 'checked_at': now
        }
        upsert(con, row)

    rows = con.execute('SELECT name,website,state,market,owner_name,owner_role,owner_confidence,email,phone,source_url,score,checked_at FROM leads ORDER BY score DESC, name').fetchall()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['dispensary','website','state','market','owner_name','owner_role','owner_confidence','email','phone','source_url','score','checked_at'])
        for r in rows:
            w.writerow(r)
    print(f'Wrote {len(rows)} leads -> {OUT}')

if __name__ == '__main__':
    main()
