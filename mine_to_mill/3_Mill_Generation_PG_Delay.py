"""
3_Mill_Generation_PG_Delay.py
Mill feed tracking — PostgreSQL output version (delay parameters configurable)

Differences from 5_Mill_Generation_PG.py:
  - Silo → mill delay is read from delay.config (node45_mill: t8t9t10_a ~ f)
  - Each mill line uses its own delay parameter; initially all equal (15 minutes)
"""

import os
import pandas as pd
import json
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

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
        return {"node45_mill": {f"t8t9t10_{chr(ord('a')+i)}": 15.0 for i in range(6)}}

_DELAY_CFG = _load_delay_config()
_MILL_T8T9T10_KEY = {i: 't8t9t10_' + chr(ord('a') + i - 1) for i in range(1, 7)}

def get_mill_delay(mill_num: int) -> float:
    """Silo → mill delay (T8+T9+T10), looked up by mill number."""
    return _DELAY_CFG['node45_mill'][_MILL_T8T9T10_KEY[mill_num]]

print("✅ Mill delay config loaded:", {m: get_mill_delay(m) for m in range(1, 7)})

MILL_TO_SILOS = {
    1: [1, 2, 3],
    2: [4, 5, 6],
    3: [7, 8, 9],
    4: [10, 11, 12],
    5: [13, 14, 15],
    6: [16, 17, 18]
}


def get_db_engine():
    engine = create_engine(DB_CONNECTION, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Database connected successfully")
    return engine


def ensure_schema(engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS mill_feed (
        time                          TIMESTAMP NOT NULL,
        mill_num                      INTEGER   NOT NULL,
        mill_throughput               DOUBLE PRECISION,
        calculated_throughput         DOUBLE PRECISION,
        mill_composition_numbers      INTEGER,
        mill_composition_tons         JSONB,
        mill_composition_pct          JSONB,
        mill_composition_properties   JSONB,
        silo1_num                     INTEGER,
        silo1_discharge               DOUBLE PRECISION,
        silo1_composition_tons        JSONB,
        silo1_composition_pct         JSONB,
        silo2_num                     INTEGER,
        silo2_discharge               DOUBLE PRECISION,
        silo2_composition_tons        JSONB,
        silo2_composition_pct         JSONB,
        silo3_num                     INTEGER,
        silo3_discharge               DOUBLE PRECISION,
        silo3_composition_tons        JSONB,
        silo3_composition_pct         JSONB,
        PRIMARY KEY (time, mill_num)
    );
    CREATE INDEX IF NOT EXISTS idx_mill_feed_time ON mill_feed (time);
    CREATE INDEX IF NOT EXISTS idx_mill_feed_num  ON mill_feed (mill_num);
    """
    migrate = """
    ALTER TABLE IF EXISTS mill_feed ADD COLUMN IF NOT EXISTS mill_composition_properties JSONB;
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.execute(text(migrate))
        conn.commit()
    print("✅ mill_feed table ready")


def write_to_pg(engine, records, overwrite, start_date, end_date):
    if not records:
        return
    df = pd.DataFrame(records)
    if df.empty:
        return

    if overwrite:
        with engine.connect() as conn:
            conn.execute(text(
                "DELETE FROM mill_feed WHERE time >= :t1 AND time <= :t2"
            ), {'t1': start_date, 't2': end_date})
            conn.commit()
        print(f"  [overwrite] Deleted existing rows for {start_date} – {end_date}")
    else:
        with engine.connect() as conn:
            existing = pd.read_sql(
                text("SELECT time, mill_num FROM mill_feed "
                     "WHERE time >= :t1 AND time <= :t2"),
                conn, params={'t1': start_date, 't2': end_date}
            )
        if not existing.empty:
            existing['key'] = existing['time'].astype(str) + '_' + existing['mill_num'].astype(str)
            df['key'] = df['time'].astype(str) + '_' + df['mill_num'].astype(str)
            before = len(df)
            df = df[~df['key'].isin(existing['key'])].drop(columns=['key'])
            print(f"  Skipped existing: {before - len(df)} rows | New rows to write: {len(df)}")
            if df.empty:
                return
        df = df.drop(columns=['key'], errors='ignore')

    # Serialize JSON columns to strings
    json_cols = [
        'mill_composition_tons', 'mill_composition_pct', 'mill_composition_properties',
        'silo1_composition_tons', 'silo1_composition_pct',
        'silo2_composition_tons', 'silo2_composition_pct',
        'silo3_composition_tons', 'silo3_composition_pct',
    ]
    for col in json_cols:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x
            )

    # psycopg2 compatibility: convert np.float64/np.integer in object columns to native Python types
    df = df.where(df.notna(), None)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(
            lambda x: float(x) if isinstance(x, np.floating) else
                     (int(x) if isinstance(x, np.integer) else x)
        )

    chunk = 1000
    for i in range(0, len(df), chunk):
        df.iloc[i:i + chunk].to_sql(
            'mill_feed', engine, if_exists='append', index=False, method='multi'
        )
    print(f"  ✅ mill_feed write complete ({len(df)} rows)")


# ── Data loading (from PostgreSQL) ────────────────────────────────────────────

def load_mill_data(engine, start_date, end_date):
    """Load mill throughput data from production_lines."""
    mill_cols = [f'n{m}num线自磨机处理量t_h' for m in range(1, 7)]
    quoted = ', '.join([f'"{c}"' for c in mill_cols])

    with engine.connect() as conn:
        mill_data = pd.read_sql(text(f"""
            SELECT "时间" AS time, {quoted}
            FROM production_lines
            WHERE "时间" >= :start AND "时间" <= :end
            ORDER BY "时间"
        """), conn, params={'start': start_date, 'end': end_date})

    mill_data['time'] = pd.to_datetime(mill_data['time'])
    for m in range(1, 7):
        old_col = f'n{m}num线自磨机处理量t_h'
        new_col = f'{m}#线自磨机处理量t/min'
        if old_col in mill_data.columns:
            mill_data[new_col] = pd.to_numeric(mill_data[old_col], errors='coerce').fillna(0) / 60
            mill_data = mill_data.drop(columns=[old_col])

    return mill_data


def load_silo_data(engine, start_date, end_date):
    """Load silo data from silo_tracking, organised by silo number."""
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT time, silo_num,
                   discharge_amount,
                   discharge_composition_tons,
                   discharge_composition_pct,
                   discharge_composition_properties
            FROM silo_tracking
            WHERE time >= :start AND time <= :end
            ORDER BY time, silo_num
        """), conn, params={'start': start_date - timedelta(minutes=max(get_mill_delay(m) for m in range(1, 7))),
                             'end': end_date})

    df['time'] = pd.to_datetime(df['time'])

    # Parse JSON columns
    # JSONB columns come back as Python dicts when read via SQLAlchemy/psycopg2;
    # fall back to json.loads() only if the value is a plain string (legacy path).
    for col in ['discharge_composition_tons', 'discharge_composition_pct', 'discharge_composition_properties']:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: x if isinstance(x, dict) else (json.loads(x) if isinstance(x, str) and x else {})
            )

    # Group into a dict keyed by silo number
    silo_tables = {}
    for silo_num in range(1, 19):
        silo_tables[silo_num] = df[df['silo_num'] == silo_num].reset_index(drop=True)

    return silo_tables


# ── Processing logic ──────────────────────────────────────────────────────────

def get_silo_data_for_mill(silo_tables, mill_time, silo_numbers, mill_num=1):
    """Get silo data for a given mill timestamp (applies T8+T9+T10 delay by mill number)."""
    silo_time = mill_time - timedelta(minutes=get_mill_delay(mill_num))
    silo_data = {}

    for silo_num in silo_numbers:
        if silo_num not in silo_tables or silo_tables[silo_num].empty:
            silo_data[silo_num] = {
                'discharge_amount': 0,
                'discharge_composition_tons': {},
                'discharge_composition_pct': {},
                'discharge_composition_properties': {}
            }
            continue

        silo_df = silo_tables[silo_num]
        row = silo_df[silo_df['time'] == silo_time]

        if not row.empty:
            silo_data[silo_num] = {
                'discharge_amount': row['discharge_amount'].iloc[0],
                'discharge_composition_tons': row['discharge_composition_tons'].iloc[0],
                'discharge_composition_pct': row['discharge_composition_pct'].iloc[0],
                'discharge_composition_properties': row['discharge_composition_properties'].iloc[0] if 'discharge_composition_properties' in row.columns else {}
            }
        else:
            silo_data[silo_num] = {
                'discharge_amount': 0,
                'discharge_composition_tons': {},
                'discharge_composition_pct': {},
                'discharge_composition_properties': {}
            }

    return silo_data


def calculate_mill_compositions(silo_data):
    total_composition = {}
    for silo_info in silo_data.values():
        for source, amount in silo_info['discharge_composition_tons'].items():
            total_composition[source] = total_composition.get(source, 0) + amount

    total_tons = sum(total_composition.values())
    composition_pct = {}
    if total_tons > 0:
        composition_pct = {k: v / total_tons * 100 for k, v in total_composition.items()}

    return {
        'composition_numbers': len(total_composition),
        'composition_tons': total_composition,
        'composition_pct': composition_pct
    }


def aggregate_mill_properties(silo_data):
    """Weighted average of silo discharge_composition_properties by discharge tonnage."""
    prop_numerator = {}
    prop_denominator = {}

    for silo_info in silo_data.values():
        tons = silo_info.get('discharge_amount', 0) or 0
        props = silo_info.get('discharge_composition_properties') or {}
        if tons <= 0 or not props:
            continue
        for k, v in props.items():
            if v is not None:
                prop_numerator[k] = prop_numerator.get(k, 0.0) + tons * v
                prop_denominator[k] = prop_denominator.get(k, 0.0) + tons

    return {
        k: round(prop_numerator[k] / prop_denominator[k], 4)
        for k in prop_numerator if prop_denominator.get(k, 0) > 0
    }


def generate_mill_feed_data(start_date, end_date, overwrite=False):
    engine = get_db_engine()
    ensure_schema(engine)

    print(f"\nLoading data {start_date.date()} ~ {end_date.date()}...")
    mill_data = load_mill_data(engine, start_date, end_date)
    silo_tables = load_silo_data(engine, start_date, end_date)

    print(f"Mill data: {len(mill_data)} rows")
    for silo_num, df in silo_tables.items():
        if not df.empty:
            print(f"  Silo {silo_num}: {len(df)} rows")

    mill_times = mill_data['time'].unique()
    total = len(mill_times)
    print(f"\n{total} timestamps to process, starting...")

    all_records = []

    for i, mill_time in enumerate(mill_times):
        if (i + 1) % 200 == 0 or (i + 1) == total:
            print(f"  Progress: {i + 1}/{total} ({(i + 1) / total * 100:.1f}%)")

        mill_row = mill_data[mill_data['time'] == mill_time]

        for mill_num, silo_numbers in MILL_TO_SILOS.items():
            mill_throughput = 0
            col = f'{mill_num}#线自磨机处理量t/min'
            if not mill_row.empty and col in mill_row.columns:
                val = mill_row[col].iloc[0]
                mill_throughput = float(val) if pd.notna(val) else 0

            silo_data = get_silo_data_for_mill(silo_tables, mill_time, silo_numbers, mill_num)
            mill_comp = calculate_mill_compositions(silo_data)
            calc_throughput = sum(s['discharge_amount'] for s in silo_data.values())

            record = {
                'time': mill_time,
                'mill_num': mill_num,
                'mill_throughput': mill_throughput,
                'calculated_throughput': calc_throughput,
                'mill_composition_numbers': mill_comp['composition_numbers'],
                'mill_composition_tons': mill_comp['composition_tons'],
                'mill_composition_pct': mill_comp['composition_pct'],
                'mill_composition_properties': aggregate_mill_properties(silo_data),
            }

            for idx, silo_num in enumerate(silo_numbers, start=1):
                record[f'silo{idx}_num'] = silo_num
                record[f'silo{idx}_discharge'] = silo_data[silo_num]['discharge_amount']
                record[f'silo{idx}_composition_tons'] = silo_data[silo_num]['discharge_composition_tons']
                record[f'silo{idx}_composition_pct'] = silo_data[silo_num]['discharge_composition_pct']

            all_records.append(record)

    write_to_pg(engine, all_records, overwrite, start_date, end_date)
    engine.dispose()
    print("\n✅ mill_feed generation complete!")


if __name__ == "__main__":
    start_date = datetime(2026, 4,  1, 0, 0, 0)
    end_date   = datetime(2026, 4, 14, 23, 59, 59)
    overwrite  = True
    generate_mill_feed_data(start_date, end_date, overwrite)
