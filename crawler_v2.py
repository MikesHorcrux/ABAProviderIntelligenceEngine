#!/usr/bin/env python3
import csv, re, sqlite3, time, json, argparse, hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib import robotparser

BASE = Path(__file__).resolve().parent
CFG_PATH = BASE / 'crawler_config.json'
SEEDS_PATH = BASE / 'seeds.csv'
DB_PATH = BASE / 'data/leads_v2.db'
OUT_DIR = BASE / 'out'

EMAIL_RE = re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}')
PHONE_RE = re.compile(r'(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')
ROLE_RE = re.compile(r'\b(founder|co[- ]?founder|owner|ceo|president|chief executive officer|coo|vp operations|director of operations|inventory manager|purchasing manager|general manager|gm)\b', re.I)
NAME_ROLE_RE = re.compile(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})[^\n\r]{0,80}\b(founder|owner|ceo|president|coo|gm|general manager)\b', re.I)
HREF_RE = re.compile(r'href=["\']([^"\'#]+)', re.I)

DEFAULT_CFG = {
  'userAgent': 'Mozilla/5.0 (compatible; LunaLeadCrawler/2.0; +local)',
  'timeoutSeconds': 6,
  'maxRetries': 2,
  'retryDelaySeconds': 1.0,
  'crawlDelaySeconds': 0.25,
  'maxDepth': 2,
  'maxPagesPerDomain': 40,
  'respectRobots': True,
  'allowedSchemes': ['http', 'https'],
  'seedFile': str(SEEDS_PATH)
}


@dataclass
class Seed:
  name: str
  website: str
  state: str
  market: str


def load_cfg():
  if not CFG_PATH.exists():
    CFG_PATH.write_text(json.dumps(DEFAULT_CFG, indent=2))
    return DEFAULT_CFG
  try:
    cfg = json.loads(CFG_PATH.read_text())
    merged = DEFAULT_CFG.copy(); merged.update(cfg)
    return merged
  except Exception:
    return DEFAULT_CFG


def norm_url(url: str) -> str:
  try:
    p = urlparse(url.strip())
    if not p.scheme:
      p = urlparse('https://' + url.strip())
    scheme = p.scheme.lower()
    host = (p.netloc or '').lower()
    if host.startswith('www.'):
      host = host[4:]
    path = p.path or '/'
    if path != '/' and path.endswith('/'):
      path = path[:-1]
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=False) if not k.lower().startswith('utm_') and k.lower() not in {'fbclid','gclid'}]
    query = urlencode(sorted(q))
    return urlunparse((scheme, host, path, '', query, ''))
  except Exception:
    return url


def same_domain(a: str, b: str) -> bool:
  pa, pb = urlparse(a), urlparse(b)
  ha, hb = (pa.netloc or '').lower(), (pb.netloc or '').lower()
  return ha == hb


def fetch(url: str, cfg) -> str:
  for i in range(cfg['maxRetries'] + 1):
    try:
      req = Request(url, headers={'User-Agent': cfg['userAgent']})
      with urlopen(req, timeout=cfg['timeoutSeconds']) as r:
        ct = (r.headers.get('Content-Type') or '').lower()
        if 'text/html' not in ct:
          return ''
        return r.read().decode('utf-8', errors='ignore')
    except (HTTPError, URLError, TimeoutError):
      if i >= cfg['maxRetries']:
        return ''
      time.sleep(cfg['retryDelaySeconds'])
    except Exception:
      return ''
  return ''


def strip_html(html: str) -> str:
  t = re.sub(r'<script[\s\S]*?</script>', ' ', html, flags=re.I)
  t = re.sub(r'<style[\s\S]*?</style>', ' ', t, flags=re.I)
  t = re.sub(r'<[^>]+>', ' ', t)
  t = re.sub(r'\s+', ' ', t)
  return t.strip()


def extract_links(base_url: str, html: str):
  out = []
  for raw in HREF_RE.findall(html):
    u = urljoin(base_url, raw)
    nu = norm_url(u)
    p = urlparse(nu)
    if p.scheme not in {'http','https'}:
      continue
    out.append(nu)
  return out


def score(owner, role, email, phone, pages):
  s = 0
  if owner: s += 40
  if role: s += 20
  if email: s += 20
  if phone: s += 10
  if pages >= 5: s += 10
  return min(100, s)


def confidence(s):
  return 'High' if s >= 75 else ('Medium' if s >= 45 else 'Low')


def init_db():
  DB_PATH.parent.mkdir(parents=True, exist_ok=True)
  con = sqlite3.connect(DB_PATH)
  con.execute('''CREATE TABLE IF NOT EXISTS crawl_pages (
    id INTEGER PRIMARY KEY,
    run_id TEXT,
    seed_name TEXT,
    url TEXT,
    depth INTEGER,
    fetched_at TEXT,
    ok INTEGER,
    content_hash TEXT
  )''')
  con.execute('''CREATE TABLE IF NOT EXISTS leads (
    id INTEGER PRIMARY KEY,
    run_id TEXT,
    dispensary TEXT,
    website TEXT,
    state TEXT,
    market TEXT,
    owner_name TEXT,
    owner_role TEXT,
    owner_confidence TEXT,
    email TEXT,
    phone TEXT,
    source_url TEXT,
    source_snippet TEXT,
    pages_crawled INTEGER,
    score INTEGER,
    checked_at TEXT
  )''')
  con.commit()
  return con


def load_seeds(path: Path):
  rows = list(csv.DictReader(path.open()))
  seeds = []
  for r in rows:
    seeds.append(Seed((r.get('name') or '').strip(), norm_url((r.get('website') or '').strip()), (r.get('state') or '').strip(), (r.get('market') or '').strip()))
  return [s for s in seeds if s.website]


def crawl_seed(seed: Seed, cfg, con, run_id):
  start = seed.website
  rp = robotparser.RobotFileParser()
  robots_ok = True
  if cfg.get('respectRobots', True):
    try:
      robots_url = urljoin(start, '/robots.txt')
      req = Request(robots_url, headers={'User-Agent': cfg['userAgent']})
      with urlopen(req, timeout=cfg['timeoutSeconds']) as r:
        data = r.read().decode('utf-8', errors='ignore')
      rp.parse(data.splitlines())
    except Exception:
      pass

  q = [(start, 0)]
  seen = set()
  pages = []
  best = {'owner_name':'', 'owner_role':'', 'email':'', 'phone':'', 'source_url':start, 'source_snippet':''}

  while q:
    url, depth = q.pop(0)
    if url in seen: continue
    seen.add(url)
    if len(seen) > cfg['maxPagesPerDomain']: break
    if cfg.get('respectRobots', True):
      try:
        if not rp.can_fetch(cfg['userAgent'], url):
          continue
      except Exception:
        pass

    html = fetch(url, cfg)
    ok = 1 if html else 0
    txt = strip_html(html) if html else ''
    h = hashlib.sha1((txt[:1000]).encode('utf-8', errors='ignore')).hexdigest() if txt else ''
    con.execute('INSERT INTO crawl_pages (run_id,seed_name,url,depth,fetched_at,ok,content_hash) VALUES (?,?,?,?,?,?,?)',
                (run_id, seed.name, url, depth, datetime.now().isoformat(timespec='seconds'), ok, h))
    con.commit()
    pages.append(url)

    if txt:
      emails = [e for e in EMAIL_RE.findall(txt) if not e.lower().endswith(('.png','.jpg','.jpeg','.webp','.gif'))]
      phones = PHONE_RE.findall(txt)
      nm = NAME_ROLE_RE.search(txt)
      rm = ROLE_RE.search(txt)
      owner = nm.group(1).strip() if nm else ''
      role = nm.group(2).strip() if nm else (rm.group(1) if rm else '')
      snippet = txt[:220]
      cand = {
        'owner_name': owner[:120],
        'owner_role': role[:90],
        'email': (emails[0] if emails else '')[:160],
        'phone': (phones[0] if phones else '')[:40],
        'source_url': url,
        'source_snippet': snippet,
      }
      if score(cand['owner_name'], cand['owner_role'], cand['email'], cand['phone'], len(pages)) > score(best['owner_name'], best['owner_role'], best['email'], best['phone'], len(pages)):
        best = cand

      if depth < cfg['maxDepth']:
        for lk in extract_links(url, html):
          if same_domain(start, lk):
            q.append((lk, depth + 1))

    time.sleep(cfg['crawlDelaySeconds'])

  sc = score(best['owner_name'], best['owner_role'], best['email'], best['phone'], len(pages))
  row = {
    'run_id': run_id,
    'dispensary': seed.name,
    'website': seed.website,
    'state': seed.state,
    'market': seed.market,
    'owner_name': best['owner_name'],
    'owner_role': best['owner_role'],
    'owner_confidence': confidence(sc),
    'email': best['email'],
    'phone': best['phone'],
    'source_url': best['source_url'],
    'source_snippet': best['source_snippet'],
    'pages_crawled': len(pages),
    'score': sc,
    'checked_at': datetime.now().isoformat(timespec='seconds')
  }
  return row


def write_exports(con):
  OUT_DIR.mkdir(parents=True, exist_ok=True)
  rows = con.execute('''SELECT dispensary,website,state,market,owner_name,owner_role,owner_confidence,email,phone,source_url,source_snippet,pages_crawled,score,checked_at
                        FROM leads ORDER BY score DESC, pages_crawled DESC, dispensary''').fetchall()

  raw = OUT_DIR / 'raw_leads.csv'
  with raw.open('w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['dispensary','website','state','market','owner_name','owner_role','owner_confidence','email','phone','source_url','source_snippet','pages_crawled','score','checked_at'])
    w.writerows(rows)

  cand = OUT_DIR / 'candidate_buyers.csv'
  with cand.open('w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['dispensary','website','state','market','owner_name','owner_role','owner_confidence','email','phone','source_url','score'])
    for r in rows:
      if r[7] or r[4] or r[5]:
        w.writerow([r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[12]])

  out100 = OUT_DIR / 'outreach_100.csv'
  with out100.open('w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['dispensary','website','state','market','owner_name','owner_role','owner_confidence','email','phone','source_url','score'])
    for r in rows[:100]:
      w.writerow([r[0],r[1],r[2],r[3],r[4],r[5],r[6],r[7],r[8],r[9],r[12]])


def main():
  ap = argparse.ArgumentParser()
  ap.add_argument('--mode', choices=['full','incremental'], default='full')
  args = ap.parse_args()

  cfg = load_cfg()
  con = init_db()
  run_id = datetime.now().strftime('%Y%m%d-%H%M%S')
  seeds = load_seeds(Path(cfg['seedFile']))

  print(f'Run {run_id} | mode={args.mode} | seeds={len(seeds)}')
  for i, seed in enumerate(seeds, 1):
    print(f'[{i}/{len(seeds)}] Crawling {seed.name} -> {seed.website}', flush=True)
    row = crawl_seed(seed, cfg, con, run_id)
    con.execute('DELETE FROM leads WHERE website=?', (seed.website,))
    con.execute('''INSERT INTO leads (run_id,dispensary,website,state,market,owner_name,owner_role,owner_confidence,email,phone,source_url,source_snippet,pages_crawled,score,checked_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (row['run_id'],row['dispensary'],row['website'],row['state'],row['market'],row['owner_name'],row['owner_role'],row['owner_confidence'],row['email'],row['phone'],row['source_url'],row['source_snippet'],row['pages_crawled'],row['score'],row['checked_at']))
    con.commit()

  write_exports(con)
  total = con.execute('SELECT COUNT(*) FROM leads').fetchone()[0]
  print(f'Done. leads={total} exports in {OUT_DIR}')

if __name__ == '__main__':
  main()
