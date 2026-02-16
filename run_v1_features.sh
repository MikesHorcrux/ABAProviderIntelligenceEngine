#!/bin/zsh
set -e
cd /Users/lunavanamburg/.openclaw/workspace/leads_engine
PYTHONPATH=/Users/lunavanamburg/.openclaw/workspace/leads_engine python3 jobs/ingest_sources.py
./run_v4.sh
