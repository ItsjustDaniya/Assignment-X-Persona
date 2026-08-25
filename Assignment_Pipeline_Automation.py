#!/usr/bin/env python3
"""
Assignment Pipeline
Auto-generated cron pipeline — WOW assignment tracking

Ported from the DS_batches_data notebook, wrapped for unattended GitHub
Actions execution:
  - Auth via a Metabase API key (X-Api-Key header) instead of the old
    username/password session-login flow — no more ASHRITHA_SECRET_KEY,
    no login POST, no token refresh step.
  - requests.post is patched to use a retry-hardened Session (connection
    resets / 5xx / 429 are retried automatically), matching the fix applied
    to the main Assignment Automation Pipeline for card 9913-style failures.
  - Any uncaught exception exits non-zero so the GitHub Actions run goes red.
"""

import os
import sys
import json
import time
import traceback

import requests
import pandas as pd
import gspread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

start_time = time.time()

# -------------------- ENV & AUTH --------------------
METABASE_API_KEY = os.getenv("METABASE_API_KEY")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")

missing = [n for n, v in [
    ("METABASE_API_KEY", METABASE_API_KEY),
    ("SERVICE_ACCOUNT_JSON", service_account_json),
] if not v]
if missing:
    raise ValueError(f"❌ Missing environment variables: {', '.join(missing)}")

service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc = gspread.authorize(creds)

METABASE_BASE = "https://metabase-lierhfgoeiwhr.newtonschool.co"

# -------------------- RETRY-HARDENED SESSION --------------------
# Same fix as the main Assignment Automation Pipeline: ConnectionError /
# ECONNRESET, 429, and 5xx are retried at the transport level instead of
# failing the whole job on the first hiccup.
SESSION = requests.Session()
_adapter = HTTPAdapter(
    max_retries=Retry(
        total=4,
        connect=4,
        read=2,
        backoff_factor=5,             # 5s, 10s, 20s, 40s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["POST", "GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    ),
    pool_connections=10,
    pool_maxsize=10,
)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

# Every requests.post(...) call in the ported notebook code below now goes
# through the retry-hardened session automatically — no need to edit each
# call site individually.
requests.post = SESSION.post

# Static header used for every Metabase API call — no login step, no
# token expiry/refresh to worry about.
METABASE_HEADERS = {
    "Content-Type": "application/json",
    "X-Api-Key": METABASE_API_KEY,
}

print("🔎 ENV CHECK")
print(f"   Metabase API key   : {'[SET]' if METABASE_API_KEY else '[MISSING]'}")
print(f"   SA client_email    : {service_info.get('client_email')}")

# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE BODY (ported from notebook cells: 13,14,15)
# ═══════════════════════════════════════════════════════════════════════════
try:
    # ──────────────────────────────────────────────────────────────────────
    # Cell 13
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import requests
    from gspread_dataframe import set_with_dataframe

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 1: Groomers filter
    # ═══════════════════════════════════════════════════════════════════════════
    workbook = gc.open('Groomers')
    data1 = workbook.worksheet('Groomers').get_all_values()
    df = pd.DataFrame(data1)
    df.columns = df.iloc[0]
    df = df.iloc[1:].copy()
    df = df.rename(columns={'UserID': 'user_id'})

    filtered_df = df[
        (df['Enrolled Status'] != 'Refund Requested') &
        (df['Phase'] != 'Unavailable') &
        (df['Enrolled Status'] != 'DPD/Foreclosed')
    ].copy()
    filtered_df['user_id'] = filtered_df['user_id'].astype(str).str.strip()
    allowed_ids = filtered_df['user_id'].tolist()

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 2: Fetch BOTH Metabase cards and union them
    # ═══════════════════════════════════════════════════════════════════════════
    def fetch_card(card_id):
        r = requests.post(
            f'{METABASE_BASE}/api/card/{card_id}/query/json',
            headers=METABASE_HEADERS,
            timeout=3600
        )
        r.raise_for_status()
        d = pd.DataFrame(r.json())
        print(f"  ✓ Card {card_id}: {len(d)} rows, {len(d.columns)} cols")
        return d

    print("Fetching both cards...")
    df1 = fetch_card(11345)          # Assignments WOW
    df2 = fetch_card(11773)          # Assignments WOW 2

    # tag source so you can tell the halves apart downstream
    df1['source'] = 'WOW1'
    df2['source'] = 'WOW2'

    # guard: columns must match for a clean union
    only1 = set(df1.columns) - set(df2.columns)
    only2 = set(df2.columns) - set(df1.columns)
    if only1 or only2:
        print(f"  ⚠️ Column mismatch — only in WOW1: {only1} | only in WOW2: {only2}")

    # union (outer concat keeps any non-overlapping cols as NaN rather than erroring)
    df_assignment = pd.concat([df1, df2], ignore_index=True, sort=False)
    print(f"✓ Unioned: {len(df_assignment)} rows")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 3: Filter — 2026 batches, approved students
    # ═══════════════════════════════════════════════════════════════════════════
    df_assignment['user_id'] = df_assignment['user_id'].astype(str).str.strip()
    df_assignment = df_assignment[df_assignment['admin_unit_name'].str.contains('2026', na=False)]
    df_assignment = df_assignment[df_assignment['user_id'].isin(allowed_ids)]
    print(f"✓ Filtered to {len(df_assignment)} rows")

    # NOTE: forward-fill (old Step 3) removed — the SQL now computes cumulatives
    # on the complete grid via activity-week bucketing, so forward-fill would double-count.

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 4: Merge with Master Data
    # ═══════════════════════════════════════════════════════════════════════════
    print("Merging with Master Data...")
    data1 = gc.open('DS Full program - All Intake 2026').worksheet('Master Data 2023-2026').get_all_values()
    dfm = pd.DataFrame(data1)
    dfm.columns = dfm.iloc[0]
    dfm = dfm.iloc[1:].copy()
    dfm = dfm.rename(columns={'User ID ': 'user_id'})
    dfm = dfm[~dfm['Persona'].isin(['NF', '#N/A']) & dfm['Persona'].notna()].copy()
    dfm = dfm[dfm['Batch'].str.contains('2025|2026', na=False)]
    dfm['user_id'] = dfm['user_id'].astype(str).str.strip()

    # suffixes avoid Batch / other column collisions with df_assignment
    df_assign = pd.merge(df_assignment, dfm, on='user_id', how='left', suffixes=('', '_master'))
    df_assign = df_assign.drop(columns=[c for c in df_assign.columns if c.endswith('_master')], errors='ignore')
    print(f"✓ Merged: {len(df_assign)} rows")

    # ═══════════════════════════════════════════════════════════════════════════
    # STEP 5: Write to Google Sheets
    # ═══════════════════════════════════════════════════════════════════════════
    print("Uploading...")
    sheet = gc.open_by_key('1-ZiZ5gyYKWK49GuFqqIDNobIr60Gek7DLsmfAhHbmTQ')
    ws = sheet.worksheet("Student-Assign-WOW")
    ws.clear()
    set_with_dataframe(ws, df_assign, include_index=False, include_column_header=True)

    print("\n" + "="*60)
    print("✅ UPLOAD SUCCESSFUL")
    print(f"Rows: {len(df_assign)} | Columns: {len(df_assign.columns)}")
    print(f"  WOW1: {(df_assign['source']=='WOW1').sum()} | WOW2: {(df_assign['source']=='WOW2').sum()}")
    print("="*60)

    # ──────────────────────────────────────────────────────────────────────
    # Cell 14 — manual re-write to same sheet, kept as-is
    # ──────────────────────────────────────────────────────────────────────
    sheet = gc.open_by_key('1-ZiZ5gyYKWK49GuFqqIDNobIr60Gek7DLsmfAhHbmTQ')
    ws = sheet.worksheet("Student-Assign-WOW")
    ws.clear()
    set_with_dataframe(ws, df_assign, include_index=False, include_column_header=True)


    # ──────────────────────────────────────────────────────────────────────
    # Cell 15 — second sheet target
    # ──────────────────────────────────────────────────────────────────────
    sheet = gc.open_by_key('1-ZiZ5gyYKWK49GuFqqIDNobIr60Gek7DLsmfAhHbmTQ')
    ws = sheet.worksheet("Student-Assign-WOW")
    ws.clear()
    set_with_dataframe(ws, df_assign, include_index=False, include_column_header=True)

except Exception as e:
    print(f"❌ Pipeline failed: {e}")
    traceback.print_exc()
    sys.exit(1)

mins, secs = divmod(time.time() - start_time, 60)
print(f"\n🎯 Assignment Pipeline completed successfully in {int(mins)}m {int(secs)}s")
sys.exit(0)
