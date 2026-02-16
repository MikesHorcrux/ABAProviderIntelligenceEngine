# CannaRadar V2 Backlog (Post-V1)

Created: 2026-02-16
Owner: Mike + Luna
Status: Backlog (do after V1 is feature-complete)

## Objective
Turn CannaRadar from a high-quality lead engine into a full GTM intelligence system that improves outreach performance over time.

---

## V2 Feature Set

### 1) Intent Signals Layer
Detect likely near-term buying urgency:
- Hiring signals (inventory/ops roles)
- New store openings / expansion hints
- Menu volatility (large changes in product/category counts)
- Recent operational updates/news mentions

**Output:** `intent_score`, `intent_reasons[]`, `last_intent_refresh`

---

### 2) Outreach Personalization Generator
Create lead-specific outreach context using evidence:
- “Why now” angle
- “Why you” angle
- Objection-aware opener

**Output per lead:**
- `personalization_snippet`
- `recommended_opening_line`
- `evidence_used[]`

---

### 3) Contact Strategy Engine
Choose best first channel and fallback path:
- Phone-first vs email-first vs form-first
- Backup sequence if no response

**Output:**
- `primary_channel`
- `fallback_channel`
- `contact_sequence_plan`

---

### 4) Freshness & Decay Model
Reduce stale-data risk automatically:
- Contact confidence decay over time
- Freshness SLA checks
- Re-validation queue for aging records

**Output:**
- `freshness_score`
- `stale_flag`
- `reverify_by`

---

### 5) Human Review Queue
Low-confidence or ambiguous records routed for manual triage:
- Segment ambiguity
n- Owner-role uncertainty
- Contradictory source evidence

**Output:** `needs_review.csv` with strict reason codes

---

### 6) Outcome Learning Loop
Use outreach outcomes to improve ranking:
- Bounce => penalize contact confidence
- Reply / call confirm => boost contact/entity confidence
- “No exports” => qualification gate flag

**Output:**
- dynamic confidence updates
- model/rules feedback for future scoring

---

### 7) Territory Planner
Batch leads for execution efficiency:
- geography-based daily call lists
- territory rotation support

**Output:** `territory_daily_plan.csv`

---

### 8) Quality SLA Dashboard
Track objective quality over time:
- Segment purity %
- Verified contact path %
- Freshness compliance %
- Daily net-new qualified leads

**Output:** `quality_dashboard.md` / `quality_metrics.json`

---

## V2 Exit Criteria
- Intent scoring active on all outreach leads
- Contact strategy generated for each lead
- Freshness decay + reverify loop running
- Verification outcomes updating confidence automatically
- Weekly quality KPI trend report generated

---

## Notes
- V2 begins only after V1 feature set is complete and stable.
- Keep V2 modular; avoid breaking the V1 production pipeline.
