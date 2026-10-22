#!/usr/bin/env python3
"""
Assignment Pipeline
Auto-generated cron pipeline — WOW assignment tracking

Ported from the DS_batches_data notebook, wrapped for unattended GitHub
Actions execution:
  - Auth via a Metabase API key (X-Api-Key header).
  - Card 11345 is fetched in CHUNKS of batches (via its {{Batch}} filter)
    instead of one giant request. One request over all ~89 courses ran long
    enough that the connection was dropped mid-query
    ("RemoteDisconnected: Remote end closed connection without response").
  - Transport-level retries no longer re-fire the heavy POST on a dropped
    connection (read=0). fetch_card's own loop now catches ConnectionError /
    Timeout too, and waits longer between attempts so Metabase can recover.
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

sys.stdout.reconfigure(line_buffering=True)

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
METABASE_DB_ID = 4

# How many LU batches to send to card 11345 per request. Lower = smaller,
# faster requests (more of them). 89 courses / 8 ≈ 12 requests.
BATCH_CHUNK_SIZE = 8

# Saved Metabase question returning one column `title` = every LU batch that
# card 11345 covers. Its SQL must use the same course_structure_id list as
# card 11345. SET THIS to the new card's ID.
# Template-tag id of card 11345's {{Batch}} filter (from the card definition).
BATCH_PARAM_ID = "74b13c96-585e-4f58-9d14-62556e31d95e"
BATCH_LIST_CARD_ID = int(os.getenv("BATCH_LIST_CARD_ID", "13532"))
if not BATCH_LIST_CARD_ID:
    raise ValueError("❌ Set BATCH_LIST_CARD_ID (env var or in the script) to the batch-list card's ID")

# -------------------- RETRY-HARDENED SESSION --------------------
# Transport-level retries cover connection failures and 429/5xx status codes
# only. read=0: if the server drops the connection mid-query we do NOT
# immediately re-fire the same heavy query (that just piles load on
# Metabase). fetch_card's own loop handles that case with a longer wait.
SESSION = requests.Session()
_adapter = HTTPAdapter(
    max_retries=Retry(
        total=4,
        connect=4,
        read=0,
        status=3,
        backoff_factor=5,             # 5s, 10s, 20s
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
requests.post = SESSION.post

METABASE_HEADERS = {
    "Content-Type": "application/json",
    "X-Api-Key": METABASE_API_KEY,
}
# Export endpoints take parameters form-encoded, so no JSON content type here.
METABASE_FORM_HEADERS = {"X-Api-Key": METABASE_API_KEY}

print("🔎 ENV CHECK")
print(f"   Metabase API key   : {'[SET]' if METABASE_API_KEY else '[MISSING]'}")
print(f"   SA client_email    : {service_info.get('client_email')}")


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════
def fetch_card(card_id, batch_titles=None, max_retries=4, retry_backoff=60):
    """Fetch a card's rows as a DataFrame.

    batch_titles: optional list of courses_course.title values passed to the
    card's {{Batch}} field filter, so the query only runs for those batches.

    Retries on: HTTP errors, 200-with-bad-JSON, and dropped connections /
    timeouts (the failure mode seen on card 11345)."""
    url = f'{METABASE_BASE}/api/card/{card_id}/query/json'
    form = None
    if batch_titles:
        form = {"parameters": json.dumps([{
            "id": BATCH_PARAM_ID,
            "type": "string/=",
            "target": ["dimension", ["template-tag", "Batch"]],
            "value": list(batch_titles),
        }])}

    label = f"card {card_id}" + (f" [{len(batch_titles)} batches]" if batch_titles else "")
    last_error = None

    for attempt in range(1, max_retries + 1):
        print(f"  → Fetching {label}" +
              (f" (attempt {attempt}/{max_retries})" if attempt > 1 else "") + "...")
        attempt_start = time.time()
        try:
            if form:
                r = SESSION.post(url, headers=METABASE_FORM_HEADERS, data=form, timeout=1800)
            else:
                r = SESSION.post(url, headers=METABASE_HEADERS, timeout=1800)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            elapsed = time.time() - attempt_start
            last_error = (f"Connection dropped/timed out after {elapsed:.1f}s: "
                          f"{type(e).__name__}: {e}")
            r = None

        if r is not None:
            elapsed = time.time() - attempt_start
            if not r.ok:
                try:
                    detail = r.json()
                except ValueError:
                    detail = r.text
                last_error = f"HTTP {r.status_code} after {elapsed:.1f}s\nResponse body: {detail}"
                # 4xx (other than 429) is a bad request, not a flaky backend —
                # retrying won't help, so fail fast.
                if 400 <= r.status_code < 500 and r.status_code != 429:
                    raise RuntimeError(f"❌ {label} rejected (not retrying): {last_error}")
            else:
                try:
                    payload = r.json()
                except ValueError:
                    last_error = (f"HTTP {r.status_code} after {elapsed:.1f}s but body was not "
                                  f"valid JSON (length {len(r.text)}): {r.text[:300]!r}")
                else:
                    d = pd.DataFrame(payload)
                    print(f"  ✓ {label}: {len(d)} rows (took {elapsed:.1f}s)")
                    return d

        if attempt < max_retries:
            print(f"  ⚠️  {label} attempt {attempt} failed: {last_error}\n"
                  f"     Retrying in {retry_backoff}s...")
            time.sleep(retry_backoff)
            retry_backoff = min(retry_backoff * 2, 300)

    raise RuntimeError(f"❌ {label} failed after {max_retries} attempts. Last error:\n{last_error}")


def list_2026_batches():
    """LU course titles that have 2026 assignments and fall under card 11345's
    structure-id list. Read from a small saved question (the API key can run
    saved cards but not ad-hoc SQL via /api/dataset)."""
    d = fetch_card(BATCH_LIST_CARD_ID, max_retries=3, retry_backoff=15)
    if 'title' not in d.columns:
        raise RuntimeError(f"Card {BATCH_LIST_CARD_ID} must return a 'title' column; got {list(d.columns)}")
    titles = sorted(d['title'].dropna().astype(str).unique().tolist())
    if not titles:
        raise RuntimeError(f"Card {BATCH_LIST_CARD_ID} returned no batch titles")
    return titles


def fetch_card_chunked(card_id, titles, chunk_size):
    frames = []
    chunks = [titles[i:i + chunk_size] for i in range(0, len(titles), chunk_size)]
    for n, chunk in enumerate(chunks, 1):
        print(f" Chunk {n}/{len(chunks)}")
        d = fetch_card(card_id, batch_titles=chunk)
        # Guard: if Metabase ignored the parameter we'd silently get every
        # batch once per chunk (duplicates). Fail loudly instead.
        if len(d) and 'batch_name' in d.columns:
            stray = set(d['batch_name'].dropna()) - set(chunk)
            if stray:
                raise RuntimeError(
                    f"Batch filter was ignored by card {card_id} — got rows for "
                    f"{list(stray)[:3]}... Check the card's Batch filter / parameter format.")
        frames.append(d)
        time.sleep(3)  # small gap so we don't hammer Metabase
    frames = [f for f in frames if len(f)]
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE BODY
# ═══════════════════════════════════════════════════════════════════════════
try:
    # STEP 1: Groomers filter
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

    # STEP 2: Fetch both cards and union them
    print("Listing 2026 batches for card 11345...")
    titles = list_2026_batches()
    print(f"✓ {len(titles)} batches → chunks of {BATCH_CHUNK_SIZE}")

    print("Fetching card 11345 (chunked)...")
    df1 = fetch_card_chunked(11345, titles, BATCH_CHUNK_SIZE)   # Assignments WOW
    print(f"✓ Card 11345 total: {len(df1)} rows")

    print("Fetching card 11773...")
    df2 = fetch_card(11773)                                     # Assignments WOW 2

    df1['source'] = 'WOW1'
    df2['source'] = 'WOW2'

    only1 = set(df1.columns) - set(df2.columns)
    only2 = set(df2.columns) - set(df1.columns)
    if only1 or only2:
        print(f"  ⚠️ Column mismatch — only in WOW1: {only1} | only in WOW2: {only2}")

    df_assignment = pd.concat([df1, df2], ignore_index=True, sort=False)
    print(f"✓ Unioned: {len(df_assignment)} rows")

    # STEP 3: Filter — 2026 batches, approved students
    df_assignment['user_id'] = df_assignment['user_id'].astype(str).str.strip()
    df_assignment = df_assignment[df_assignment['admin_unit_name'].str.contains('2026', na=False)]
    df_assignment = df_assignment[df_assignment['user_id'].isin(allowed_ids)]
    print(f"✓ Filtered to {len(df_assignment)} rows")

    # STEP 4: Merge with Master Data
    print("Merging with Master Data...")
    data1 = gc.open('DS Full program - All Intake 2026').worksheet('Master Data 2023-2026').get_all_values()
    dfm = pd.DataFrame(data1)
    dfm.columns = dfm.iloc[0]
    dfm = dfm.iloc[1:].copy()
    dfm = dfm.rename(columns={'User ID ': 'user_id'})
    dfm = dfm[~dfm['Persona'].isin(['NF', '#N/A']) & dfm['Persona'].notna()].copy()
    dfm = dfm[dfm['Batch'].str.contains('2025|2026', na=False)]
    dfm['user_id'] = dfm['user_id'].astype(str).str.strip()

    df_assign = pd.merge(df_assignment, dfm, on='user_id', how='left', suffixes=('', '_master'))
    df_assign = df_assign.drop(columns=[c for c in df_assign.columns if c.endswith('_master')], errors='ignore')
    print(f"✓ Merged: {len(df_assign)} rows")

    # STEP 5: Write to Google Sheets
    print("Uploading...")
    sheet = gc.open_by_key('1-ZiZ5gyYKWK49GuFqqIDNobIr60Gek7DLsmfAhHbmTQ')
    ws = sheet.worksheet("Student-Assign-WOW")
    ws.clear()
    set_with_dataframe(ws, df_assign, include_index=False, include_column_header=True)

    print("\n" + "=" * 60)
    print("✅ UPLOAD SUCCESSFUL")
    print(f"Rows: {len(df_assign)} | Columns: {len(df_assign.columns)}")
    print(f"  WOW1: {(df_assign['source']=='WOW1').sum()} | WOW2: {(df_assign['source']=='WOW2').sum()}")
    print("=" * 60)

except Exception as e:
    print(f"❌ Pipeline failed: {e}")
    traceback.print_exc()
    sys.exit(1)

mins, secs = divmod(time.time() - start_time, 60)
print(f"\n🎯 Assignment Pipeline completed successfully in {int(mins)}m {int(secs)}s")
sys.exit(0)
