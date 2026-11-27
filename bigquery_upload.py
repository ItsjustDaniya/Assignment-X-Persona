"""Publish a validated full snapshot; never modify the legacy dataframe."""
import os, json, re
from datetime import datetime, timezone, timedelta
from google.api_core.exceptions import NotFound
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

INTS = set('course_id user_id currently_enrolled refund_requested_students course_cancellation initially_enrolled deferred week_no_wrt_module total_question cumulative_total_question_released in_and_post_q_count in_class_q_count post_class_q_count general_q_count open_q_count attempt_q_count completed_q_count completed_q_on_time cumulative_open_total cumulative_attempt_total cumulative_completed_total'.split())
FLOATS = set('open_rate attempt_rate completion_rate_on_time open_rate_cumulative attempt_rate_cumulative completion_rate_cumulative'.split())
DATES = {'week_start_date', 'au_start_date'}
# IDs remain strings to avoid coercion and preserve join keys.
INTS -= {'user_id', 'course_id'}

def upload_to_bigquery(df):
    if df.empty:
        raise ValueError('Refusing to publish an empty snapshot')
    project = os.environ['BQ_PROJECT_ID']
    dataset = os.getenv('BQ_DATASET', 'academic_analytics')
    location = os.getenv('BQ_LOCATION', 'asia-south1')
    table = f'{project}.{dataset}.assignment_wow'
    creds = Credentials.from_service_account_info(json.loads(os.environ['SERVICE_ACCOUNT_JSON']), scopes=['https://www.googleapis.com/auth/cloud-platform'])
    client = bigquery.Client(project=project, credentials=creds, location=location)
    out = pd.DataFrame(index=df.index)
    schema = []
    for original in df.columns:
        name = re.sub(r'[^a-zA-Z0-9_]+', '_', str(original).strip()).strip('_').lower()
        if not name or name[0].isdigit(): name = 'col_' + name
        if name in out.columns: raise ValueError(f'Duplicate normalized column: {name}')
        s = df[original].replace('', None)
        if name in INTS:
            out[name] = pd.to_numeric(s, errors='raise').astype('Int64'); kind = 'INTEGER'
        elif name in FLOATS:
            out[name] = pd.to_numeric(s, errors='raise').astype('Float64'); kind = 'FLOAT'
        elif name in DATES:
            out[name] = pd.to_datetime(s, errors='raise', utc=True).dt.date; kind = 'DATE'
        else:
            out[name] = s.astype('string'); kind = 'STRING'
        schema.append(bigquery.SchemaField(name, kind, description=str(original)))
    try:
        existing = client.get_table(table)
    except NotFound:
        existing = None
    expected = [(f.name, f.field_type) for f in schema]
    if existing is not None:
        actual = [(f.name, f.field_type) for f in existing.schema]
        if actual != expected:
            raise RuntimeError('Schema drift detected; destination unchanged')
    config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
    )
    job = client.load_table_from_dataframe(out, table, job_config=config, location=location)
    job.result()
    published = client.get_table(table)
    if published.num_rows != len(out):
        raise RuntimeError('Published row count mismatch')
    print(f'BigQuery published {len(out):,} rows to {table}')
    print(f'Table expiration: {published.expires}')
    if published.expires and published.expires - datetime.now(timezone.utc) < timedelta(days=7):
        print('WARNING: table expires within 7 days; next successful load can recreate it.')
