#!/usr/bin/env python3
import csv, re, sqlite3, json, socket
from pathlib import Path
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.parse import urlparse

BASE = Path(__file__).resolve().parent
OUT = BASE / 'out'
RAW = OUT / 'raw_leads.csv'
DB = BASE / 'data/leads_v2.db'
CFG = BASE / 'enrich_config.json'

EMAIL_RE = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
PHONE_RE = re.compile(r'(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')
ROLE_RE = re.compile(r'\b(founder|co[- ]?founder|owner|ceo|president|coo|general manager|gm|inventory manager|purchasing manager|director of operations|vp operations)\b', re.I)
NAME_ROLE_RE = re.compile(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})[^\n\r]{0,60}\b(founder|owner|ceo|president|coo|general manager|gm)\b', re.I)
BAD_NAME_BITS = {'with','the','and','for','this','that','your','their','our','from','about','contact','policy','terms','privacy','information','respect','people','team','experts'}

socket.setdefaulttimeout(2)

DEFAULT = {
  'timeoutSeconds': 5,
  'maxRowsPerRun': 200,
  'extraPaths': ['/about','/about-us','/team','/leadership','/contact','/contact-us'],
  'userAgent': 'Mozilla/5.0 (compatible; LunaLeadEnricher/1.0; +local)'
}

def load_cfg():
  if not CFG.exists():
    CFG.write_text(json.dumps(DEFAULT, indent=2))
    return DEFAULT
  c = json.loads(CFG.read_text())
  d = DEFAULT.copy(); d.update(c)
  return d

def fetch(url, cfg):
  try:
    req = Request(url, headers={'User-Agent': cfg['userAgent']})
    with urlopen(req, timeout=cfg['timeoutSeconds']) as r:
      ct = (r.headers.get('Content-Type') or '').lower()
      if 'text/html' not in ct: return ''
      return r.read().decode('utf-8', errors='ignore')
  except Exception:
    return ''

def strip_html(s):
  s = re.sub(r'<script[\s\S]*?</script>', ' ', s, flags=re.I)
  s = re.sub(r'<style[\s\S]*?</style>', ' ', s, flags=re.I)
  s = re.sub(r'<[^>]+>', ' ', s)
  return re.sub(r'\s+', ' ', s).strip()

def score(owner, role, email, phone):
  s=0
  if owner: s+=45
  if role: s+=20
  if email: s+=20
  if phone: s+=15
  return min(s,100)

def conf(s):
  return 'High' if s>=75 else ('Medium' if s>=45 else 'Low')

def ensure_tables(con):
  con.execute('''CREATE TABLE IF NOT EXISTS lead_evidence (
    id INTEGER PRIMARY KEY,
    website TEXT,
    field TEXT,
    value TEXT,
    source_url TEXT,
    snippet TEXT,
    captured_at TEXT
  )''')
  con.commit()

def main():
  cfg=load_cfg()
  if not RAW.exists():
    print('No raw_leads.csv yet')
    return
  rows=list(csv.DictReader(RAW.open()))[:cfg['maxRowsPerRun']]
  con=sqlite3.connect(DB)
  ensure_tables(con)
  now=datetime.now().isoformat(timespec='seconds')
  updated=[]

  for i, r in enumerate(rows, 1):
    site=(r.get('website') or '').rstrip('/')
    print(f'Enriching [{i}/{len(rows)}] {site}', flush=True)
    if not site: continue
    best={
      'owner_name': r.get('owner_name',''),
      'owner_role': r.get('owner_role',''),
      'email': r.get('email',''),
      'phone': r.get('phone',''),
      'source_url': r.get('source_url') or site,
      'source_snippet': r.get('source_snippet','')[:240]
    }
    base_score=score(best['owner_name'],best['owner_role'],best['email'],best['phone'])
    for p in cfg['extraPaths']:
      url=site+p
      html=fetch(url,cfg)
      if not html: continue
      txt=strip_html(html)
      emails=[e for e in EMAIL_RE.findall(txt) if not e.lower().endswith(('.png','.jpg','.jpeg','.gif','.webp'))]
      phones=PHONE_RE.findall(txt)
      nm=NAME_ROLE_RE.search(txt)
      rm=ROLE_RE.search(txt)
      owner_name = (nm.group(1).strip() if nm else '')
      if owner_name:
        toks = [t.lower() for t in owner_name.split()]
        if any(t in BAD_NAME_BITS for t in toks):
          owner_name = ''
      cand={
        'owner_name': owner_name[:120],
        'owner_role': ((nm.group(2).strip() if nm else (rm.group(1) if rm else '')))[:80],
        'email': (emails[0] if emails else '')[:160],
        'phone': (phones[0] if phones else '')[:40],
        'source_url': url,
        'source_snippet': txt[:240]
      }
      s=score(cand['owner_name'],cand['owner_role'],cand['email'],cand['phone'])
      if s>base_score:
        best=cand; base_score=s

    con.execute('DELETE FROM leads WHERE website=?', (site+'/',))
    con.execute('DELETE FROM leads WHERE website=?', (site,))
    con.execute('''INSERT INTO leads (run_id,dispensary,website,state,market,owner_name,owner_role,owner_confidence,email,phone,source_url,source_snippet,pages_crawled,score,checked_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                ('enrich-'+now, r.get('dispensary') or r.get('name') or '', site, r.get('state',''), r.get('market',''), best['owner_name'], best['owner_role'], conf(base_score), best['email'], best['phone'], best['source_url'], best['source_snippet'], int(r.get('pages_crawled') or 0), base_score, now))
    for fld in ('owner_name','owner_role','email','phone'):
      val=best.get(fld,'')
      if val:
        con.execute('INSERT INTO lead_evidence (website,field,value,source_url,snippet,captured_at) VALUES (?,?,?,?,?,?)',
                    (site,fld,val,best['source_url'],best['source_snippet'],now))
    con.commit()
    updated.append(site)

  export=OUT/'enriched_leads.csv'
  q=con.execute('SELECT dispensary,website,state,market,owner_name,owner_role,owner_confidence,email,phone,source_url,score,checked_at FROM leads ORDER BY score DESC, dispensary').fetchall()
  with export.open('w', newline='') as f:
    w=csv.writer(f)
    w.writerow(['dispensary','website','state','market','owner_name','owner_role','owner_confidence','email','phone','source_url','score','checked_at'])
    w.writerows(q)
  print(f'Enriched {len(updated)} rows -> {export}')

if __name__=='__main__':
  main()
