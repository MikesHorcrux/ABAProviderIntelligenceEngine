#!/bin/zsh
set -e
cd /Users/lunavanamburg/.openclaw/workspace/leads_engine
./crawler_v2.py --mode full
./enrich.py
./postprocess_v4.py
./brief.py
