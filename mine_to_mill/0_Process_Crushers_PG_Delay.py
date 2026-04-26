"""
0_Process_Crushers_PG_Delay.py
Crusher / CVR data processing — PostgreSQL output (configurable delay parameters)

Differences from 0_Process_Crushers_PG.py:
  - Truck timestamp calibration delay is read from delay.config (node0_crusher: sum of t1~t4 per suffix)
  - Initially all crusher lines use the same value (t1a=t1b=t1c=t1d=0.5, total 2 min).
    Each line can be tuned independently after calibration.
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import os
import numpy as np
from sqlalchemy import create_engine, text
from openpyxl.utils import get_column_letter

DB_CONNECTION = 'postgresql://postgres:postgres@localhost:5432/postgres'

# ── Delay configuration ───────────────────────────────────────────────────────
_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            '..', 'resources', 'delay.config')

def _load_delay_config():
    try:
        with open(_CONFIG_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️  Failed to read delay.config, using defaults: {e}")
        return {"node0_crusher": {
            "t1a": 0.5, "t1b": 0.5, "t1c": 0.5, "t1d": 0.5,
            "t2a": 0.5, "t2b": 0.5, "t2c": 0.5, "t2d": 0.5,
            "t3a": 0.5, "t3b": 0.5, "t3c": 0.5, "t3d": 0.5,
            "t4a": 0.5, "t4b": 0.5, "t4c": 0.5, "t4d": 0.5,
        }}

def get_crusher_delay(cfg, suffix='a'):
    """Total calibration delay T1+T2+T3+T4 from truck arrival to CVR (minutes)."""
    c = cfg['node0_crusher']
    return c[f't1{suffix}'] + c[f't2{suffix}'] + c[f't3{suffix}'] + c[f't4{suffix}']

# Loaded once at startup; no crusher ID field yet, all lines use suffix 'a' (same value as b/c/d)
_DELAY_CFG    = _load_delay_config()
CRUSHER_DELAY = get_crusher_delay(_DELAY_CFG, suffix='a')
print(f"✅ Crusher calibration delay (T1+T2+T3+T4) = {CRUSHER_DELAY} min")


def get_db_engine():
    engine = create_engine(DB_CONNECTION, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Database connected successfully")
    return engine


def ensure_schema(engine):
    """Create cvr_tracking table if it does not exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS cvr_tracking (
        time                           TIMESTAMP     NOT NULL,
        cvr_name                       VARCHAR(10)   NOT NULL,
        instant_total_ore              DOUBLE PRECISION,
        dump_time                      TIMESTAMP,
        date                           DATE,
        shift                          VARCHAR(20),
        truck                          VARCHAR(20),
        dig_unit                       VARCHAR(50),
        source                         VARCHAR(100),
        bench_id                       DOUBLE PRECISION,
        shot_id                        VARCHAR(100),
        end_processor_group_reporting  TEXT,
        end_processor_group            TEXT,
        material                       TEXT,
        truck_payload_t                DOUBLE PRECISION,
        adjusted_truck_payload_t       DOUBLE PRECISION,
        start_timestamp                TIMESTAMP,
        end_timestamp                  TIMESTAMP,
        stratigraphy                   VARCHAR(20),
        geomet_domain                  VARCHAR(50),
        imt_p80                        DOUBLE PRECISION,
        dtr_pct                        DOUBLE PRECISION,
        fe_concentrate_pct             DOUBLE PRECISION,
        fe_head_pct                    DOUBLE PRECISION,
        magfe_pct                      DOUBLE PRECISION,
        magfe_dtr_pct                  DOUBLE PRECISION,
        sio2_concentrate_pct           DOUBLE PRECISION,
        survey_adjusted_factor         DOUBLE PRECISION,
        cycle_oid                      BIGINT,
        mprs_create_ts                 TIMESTAMP,
        PRIMARY KEY (time, cvr_name)
    );
    CREATE INDEX IF NOT EXISTS idx_cvr_tracking_time ON cvr_tracking (time);
    CREATE INDEX IF NOT EXISTS idx_cvr_tracking_name ON cvr_tracking (cvr_name);
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
        for sql in [
            "ALTER TABLE cvr_tracking DROP COLUMN IF EXISTS ore_waste_block",
            "ALTER TABLE cvr_tracking DROP COLUMN IF EXISTS end_processor",
        ]:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception as e:
                print(f"  Warning (cvr_tracking migration): {e}")
    print("✅ cvr_tracking table ready")


def write_to_pg(engine, df, overwrite, start_date, end_date):
    """Write data to cvr_tracking table."""
    if df.empty:
        return

    if overwrite:
        with engine.connect() as conn:
            conn.execute(text(
                "DELETE FROM cvr_tracking WHERE time >= :t1 AND time <= :t2"
            ), {'t1': start_date, 't2': end_date})
            conn.commit()
        print(f"  [overwrite] Deleted existing rows for {start_date.date()} ~ {end_date.date()}")
    else:
        with engine.connect() as conn:
            existing = pd.read_sql(
                text("SELECT time, cvr_name FROM cvr_tracking WHERE time >= :t1 AND time <= :t2"),
                conn, params={'t1': start_date, 't2': end_date}
            )
        if not existing.empty:
            existing['key'] = existing['time'].astype(str) + '_' + existing['cvr_name']
            df['key'] = df['time'].astype(str) + '_' + df['cvr_name']
            before = len(df)
            df = df[~df['key'].isin(existing['key'])].drop(columns=['key'])
            print(f"  Skipped existing: {before - len(df)} rows | New rows: {len(df)}")
            if df.empty:
                return
        df = df.drop(columns=['key'], errors='ignore')

    chunk = 1000
    for i in range(0, len(df), chunk):
        df.iloc[i:i + chunk].to_sql(
            'cvr_tracking', engine, if_exists='append', index=False, method='multi'
        )
    print(f"  ✅ cvr_tracking write complete ({len(df)} rows)")


# ── Processing logic (identical to V2_fixed) ─────────────────────────────────

def load_truck_data_for_date(engine, start_date, end_date):
    calibrated_start = start_date - timedelta(minutes=CRUSHER_DELAY)
    calibrated_end = end_date - timedelta(minutes=CRUSHER_DELAY)
    print(f"  Query time range: {calibrated_start} to {calibrated_end}")

    query = """
        SELECT *
        FROM truck_cycles
        WHERE end_timestamp >= %(start)s
          AND end_timestamp <= %(end)s
          AND end_processor_group = 'Ore - Crushed'
        ORDER BY end_timestamp
    """
    df = pd.read_sql_query(query, engine, params={'start': calibrated_start, 'end': calibrated_end})
    print(f"  Retrieved {len(df)} truck cycle records")
    return df


def process_excel(df):
    if df.empty:
        return df
    filtered_df = df[df['end_processor_group'] == 'Ore - Crushed'].copy()

    def calibrate_timestamp(row):
        try:
            original_time = row['end_timestamp']
            if isinstance(original_time, str):
                original_time = datetime.strptime(original_time, "%a %d/%m/%Y %H:%M:%S")
            return original_time - timedelta(minutes=CRUSHER_DELAY)
        except Exception as e:
            return None

    filtered_df['Calibrated Timestamp'] = filtered_df.apply(calibrate_timestamp, axis=1)
    filtered_df = filtered_df.dropna(subset=['Calibrated Timestamp'])
    return filtered_df.sort_values(by='Calibrated Timestamp')


def clean_numeric_data(series):
    def convert_value(x):
        if pd.isna(x):
            return 0
        if isinstance(x, str):
            x = x.strip()
            if x == '000000':
                return 0
            try:
                return float(x)
            except ValueError:
                return 0
        return float(x)
    return series.apply(convert_value)


def load_cvr_data_for_date(engine, start_date, end_date):
    buffer_start = start_date - timedelta(hours=1)
    buffer_end = end_date + timedelta(hours=1)

    cvr_columns = {
        'CVR111': 'n1num排料皮带矿量',
        'CVR112': 'n2num排料皮带矿量',
        'CVR113': 'n3num排料皮带矿量',
        'CVR114': 'n4num排料皮带矿量',
    }

    quoted_cols = ', '.join([f'"{c}"' for c in cvr_columns.values()])
    query = f"""
        SELECT date, {quoted_cols}
        FROM realtime_data
        WHERE date >= %(start)s AND date <= %(end)s
        ORDER BY date
    """
    df = pd.read_sql_query(query, engine, params={'start': buffer_start, 'end': buffer_end})
    df['date'] = pd.to_datetime(df['date'])

    cvr_processed_data = {}
    for cvr_name, col in cvr_columns.items():
        if col not in df.columns:
            continue
        df[col] = clean_numeric_data(df[col])
        cvr_data = df.groupby('date').agg({col: 'mean'}).reset_index()
        cvr_data[col] = cvr_data[col] / 60  # t/h → t/min
        cvr_data.columns = ['Timestamp', 'Throughput']
        cvr_processed_data[cvr_name] = cvr_data

    return cvr_processed_data


def calculate_interval_time_with_ore(cvr_data, start_time, truck_payload):
    relevant_data = cvr_data[cvr_data['Timestamp'] >= start_time].copy()
    if relevant_data.empty:
        return 1, {}

    cumulative_throughput = 0
    minutes_needed = 0
    consecutive_zeros = 0
    instant_ore_data = {}

    for _, row in relevant_data.iterrows():
        current_time = row['Timestamp']
        current_throughput = row['Throughput']
        instant_ore_data[current_time] = current_throughput

        if current_throughput == 0:
            consecutive_zeros += 1
        else:
            consecutive_zeros = 0

        if consecutive_zeros > 10:
            return max(1, minutes_needed) if minutes_needed > 0 else 1, instant_ore_data

        cumulative_throughput += current_throughput
        minutes_needed += 1

        if cumulative_throughput >= truck_payload:
            break

    return max(1, minutes_needed), instant_ore_data


# Columns from truck_cycles to carry through to cvr_tracking output
TRUCK_COLS_TO_KEEP = [
    'date', 'shift', 'truck', 'dig_unit', 'source',
    'bench_id', 'shot_id',
    'end_processor_group_reporting', 'end_processor_group',
    'material', 'truck_payload_t', 'adjusted_truck_payload_t',
    'start_timestamp', 'end_timestamp',
    'stratigraphy', 'geomet_domain', 'imt_p80',
    'dtr_pct', 'fe_concentrate_pct', 'fe_head_pct',
    'magfe_pct', 'magfe_dtr_pct', 'sio2_concentrate_pct',
    'survey_adjusted_factor', 'cycle_oid', 'mprs_create_ts'
]


def process_cvr_data_for_date(engine, start_date, end_date, overwrite=False):
    print(f"\n📊 Loading truck data for {start_date.date()}...")
    df = load_truck_data_for_date(engine, start_date, end_date)
    if df.empty:
        print(f"  ⚠️  No truck data found")
        return

    df = process_excel(df)
    if df.empty:
        print(f"  ⚠️  No crusher data after filtering")
        return

    df.rename(columns={'Calibrated Timestamp': 'Dump Time'}, inplace=True)
    df = df.sort_values('Dump Time')
    df['Date'] = df['Dump Time'].dt.date

    print(f"\n📊 Loading CVR throughput data...")
    cvr_processed_data = load_cvr_data_for_date(engine, start_date, end_date)
    if not cvr_processed_data:
        print(f"  ⚠️  No CVR data available")
        return

    cvr_mapping = {
        'CR1': 'CVR111',
        'CR2': 'CVR112',
        'CR3': 'CVR113',
        'CR4': 'CVR114'
    }

    current_date = start_date.date()
    day_start = pd.Timestamp(current_date)
    day_end = pd.Timestamp(current_date) + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)
    day_time_range = pd.date_range(start=day_start, end=day_end, freq='min')

    daily_data = df[df['Dump Time'].dt.date == current_date].sort_values('Dump Time')

    all_rows = []

    for processor, cvr_name in cvr_mapping.items():
        if cvr_name not in cvr_processed_data:
            print(f"  ⚠️  Skipping {cvr_name} - no CVR throughput data")
            continue

        daily_result = pd.DataFrame({'Current Timestamp': day_time_range})
        daily_result['InstantTotalOre'] = pd.NA
        daily_result['Dump Time'] = pd.NA
        for col in TRUCK_COLS_TO_KEEP:
            daily_result[col] = pd.NA

        last_end_time = None

        if not daily_data.empty:
            processor_data = daily_data[daily_data['end_processor'] == processor].copy()

            for _, dump_record in processor_data.iterrows():
                dump_time = dump_record['Dump Time']
                truck_payload = dump_record.get('adjusted_truck_payload_t', 0)
                if pd.isna(truck_payload):
                    truck_payload = 0

                first_current_time = dump_time + pd.Timedelta(minutes=1)
                if last_end_time and first_current_time <= last_end_time:
                    first_current_time = last_end_time + pd.Timedelta(minutes=1)
                first_current_time = first_current_time.floor('min')

                interval, instant_ore_data = calculate_interval_time_with_ore(
                    cvr_processed_data[cvr_name], first_current_time, truck_payload
                )

                end_time = first_current_time + pd.Timedelta(minutes=interval - 1)
                interval_range = pd.date_range(start=first_current_time, end=end_time, freq='min')

                for time_point in interval_range:
                    if day_start <= time_point <= day_end:
                        mask = daily_result['Current Timestamp'] == time_point
                        daily_result.loc[mask, 'Dump Time'] = dump_time
                        for col in TRUCK_COLS_TO_KEEP:
                            if col in dump_record.index:
                                daily_result.loc[mask, col] = dump_record[col]
                        if time_point in instant_ore_data:
                            daily_result.loc[mask, 'InstantTotalOre'] = instant_ore_data[time_point]

                last_end_time = end_time

        # Rename columns to match cvr_tracking table schema
        daily_result = daily_result.rename(columns={
            'Current Timestamp': 'time',
            'InstantTotalOre': 'instant_total_ore',
            'Dump Time': 'dump_time',
        })
        daily_result['cvr_name'] = cvr_name
        all_rows.append(daily_result)

    if all_rows:
        final_df = pd.concat(all_rows, ignore_index=True)
        # Convert timestamp columns
        final_df['time'] = pd.to_datetime(final_df['time'])
        final_df['dump_time'] = pd.to_datetime(final_df['dump_time'], errors='coerce')
        write_to_pg(engine, final_df, overwrite, start_date, end_date)


def process_date_range(start_date, end_date, overwrite=False):
    engine = get_db_engine()
    ensure_schema(engine)

    current_date = start_date
    success_count = 0
    fail_count = 0

    while current_date < end_date:
        next_date = min(current_date + timedelta(days=1), end_date)

        print(f"\n{'=' * 80}")
        print(f"Processing data for {current_date.date()}")
        print(f"{'=' * 80}")

        try:
            process_cvr_data_for_date(engine, current_date, next_date, overwrite)
            success_count += 1
        except Exception as e:
            print(f"\n❌ Error processing {current_date.date()}: {e}")
            import traceback
            traceback.print_exc()
            fail_count += 1

        current_date = next_date

    engine.dispose()
    print(f"\n{'=' * 80}")
    print(f"PROCESSING SUMMARY")
    print(f"{'=' * 80}")
    print(f"✅ Successful: {success_count} days")
    print(f"❌ Failed: {fail_count} days")


if __name__ == "__main__":
    start_date = datetime(2025, 4,  1, 0, 0, 0)
    end_date   = datetime(2025, 4, 14, 23, 59, 59)
    overwrite  = True
    process_date_range(start_date, end_date, overwrite)
