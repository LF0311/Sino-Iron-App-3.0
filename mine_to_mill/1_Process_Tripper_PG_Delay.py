"""
1_Process_Tripper_PG_Delay.py
Tripper data processing — PostgreSQL output version (delay parameters configurable)

Differences from 1_Process_Tripper_PG.py:
  - CVR belt transport delays (T5) and long-belt delays (T6) are read from delay.config
  - CVR111: t5a+t6a, CVR112: t5b+t6a, CVR113: t5c+t6b, CVR114: t5d+t6b
  - Initially t6a=t6b=0; all delays expressed in t5; can be split after calibration
"""

import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import numpy as np

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
        return {"node1_cvr": {
            "t5a": 3.0, "t5b": 1.0, "t5c": 3.0, "t5d": 2.0,
            "t6a": 0.0, "t6b": 0.0,
        }}

def _build_cvr_delays(cfg):
    """Return the combined transport delay (T5+T6, in minutes) for each CVR."""
    c = cfg['node1_cvr']
    return {
        'CVR111': c['t5a'] + c['t6a'],
        'CVR112': c['t5b'] + c['t6a'],
        'CVR113': c['t5c'] + c['t6b'],
        'CVR114': c['t5d'] + c['t6b'],
    }

_DELAY_CFG  = _load_delay_config()
_CVR_DELAYS = _build_cvr_delays(_DELAY_CFG)
print(f"✅ CVR transport delays (T5+T6): {_CVR_DELAYS}")


def get_db_engine():
    engine = create_engine(DB_CONNECTION, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Database connected successfully")
    return engine


def ensure_schema(engine):
    """Ensure the tripper_tracking table exists."""
    ddl = """
    CREATE TABLE IF NOT EXISTS tripper_tracking (
        time                        TIMESTAMP    NOT NULL,
        tripper_name                VARCHAR(10)  NOT NULL,
        total_ore                   DOUBLE PRECISION,
        belt_running                BOOLEAN,
        cvr1_instant_ore            DOUBLE PRECISION,
        cvr2_instant_ore            DOUBLE PRECISION,
        cvr1_dump_time              TIMESTAMP,
        cvr2_dump_time              TIMESTAMP,
        cvr1_date                           DATE,
        cvr2_date                           DATE,
        cvr1_shift                          VARCHAR(20),
        cvr2_shift                          VARCHAR(20),
        cvr1_truck                          VARCHAR(20),
        cvr2_truck                          VARCHAR(20),
        cvr1_dig_unit                       VARCHAR(50),
        cvr2_dig_unit                       VARCHAR(50),
        cvr1_source                         VARCHAR(100),
        cvr2_source                         VARCHAR(100),
        cvr1_bench_id                       VARCHAR(100),
        cvr2_bench_id                       VARCHAR(100),
        cvr1_shot_id                        VARCHAR(100),
        cvr2_shot_id                        VARCHAR(100),
        cvr1_end_processor_group_reporting  TEXT,
        cvr2_end_processor_group_reporting  TEXT,
        cvr1_end_processor_group            TEXT,
        cvr2_end_processor_group            TEXT,
        cvr1_material                       TEXT,
        cvr2_material                       TEXT,
        cvr1_truck_payload_t                DOUBLE PRECISION,
        cvr2_truck_payload_t                DOUBLE PRECISION,
        cvr1_adjusted_truck_payload_t       DOUBLE PRECISION,
        cvr2_adjusted_truck_payload_t       DOUBLE PRECISION,
        cvr1_start_timestamp                TIMESTAMP,
        cvr2_start_timestamp                TIMESTAMP,
        cvr1_end_timestamp                  TIMESTAMP,
        cvr2_end_timestamp                  TIMESTAMP,
        cvr1_stratigraphy                   VARCHAR(20),
        cvr2_stratigraphy                   VARCHAR(20),
        cvr1_geomet_domain                  VARCHAR(50),
        cvr2_geomet_domain                  VARCHAR(50),
        cvr1_imt_p80                        DOUBLE PRECISION,
        cvr2_imt_p80                        DOUBLE PRECISION,
        cvr1_dtr_pct                        DOUBLE PRECISION,
        cvr2_dtr_pct                        DOUBLE PRECISION,
        cvr1_fe_concentrate_pct             DOUBLE PRECISION,
        cvr2_fe_concentrate_pct             DOUBLE PRECISION,
        cvr1_fe_head_pct                    DOUBLE PRECISION,
        cvr2_fe_head_pct                    DOUBLE PRECISION,
        cvr1_magfe_pct                      DOUBLE PRECISION,
        cvr2_magfe_pct                      DOUBLE PRECISION,
        cvr1_magfe_dtr_pct                  DOUBLE PRECISION,
        cvr2_magfe_dtr_pct                  DOUBLE PRECISION,
        cvr1_sio2_concentrate_pct           DOUBLE PRECISION,
        cvr2_sio2_concentrate_pct           DOUBLE PRECISION,
        cvr1_survey_adjusted_factor         DOUBLE PRECISION,
        cvr2_survey_adjusted_factor         DOUBLE PRECISION,
        cvr1_cycle_oid                      BIGINT,
        cvr2_cycle_oid                      BIGINT,
        cvr1_mprs_create_ts                 TIMESTAMP,
        cvr2_mprs_create_ts                 TIMESTAMP,
        PRIMARY KEY (time, tripper_name)
    );
    CREATE INDEX IF NOT EXISTS idx_tripper_tracking_time ON tripper_tracking (time);
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
        for sql in [
            "ALTER TABLE tripper_tracking DROP COLUMN IF EXISTS cvr1_ore_waste_block",
            "ALTER TABLE tripper_tracking DROP COLUMN IF EXISTS cvr2_ore_waste_block",
            "ALTER TABLE tripper_tracking DROP COLUMN IF EXISTS cvr1_end_processor",
            "ALTER TABLE tripper_tracking DROP COLUMN IF EXISTS cvr2_end_processor",
            "ALTER TABLE tripper_tracking ALTER COLUMN cvr1_bench_id TYPE VARCHAR(100) USING cvr1_bench_id::VARCHAR",
            "ALTER TABLE tripper_tracking ALTER COLUMN cvr2_bench_id TYPE VARCHAR(100) USING cvr2_bench_id::VARCHAR",
        ]:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception as e:
                print(f"  Warning (tripper_tracking migration): {e}")
    print("✅ tripper_tracking table ready")


def write_to_pg(engine, df, overwrite, start_date, end_date):
    if df.empty:
        return
    if overwrite:
        with engine.connect() as conn:
            conn.execute(text(
                "DELETE FROM tripper_tracking WHERE time >= :t1 AND time <= :t2"
            ), {'t1': start_date, 't2': end_date})
            conn.commit()
        print(f"  [overwrite] Deleted existing rows for {start_date} – {end_date}")
    else:
        with engine.connect() as conn:
            existing = pd.read_sql(
                text("SELECT time, tripper_name FROM tripper_tracking "
                     "WHERE time >= :t1 AND time <= :t2"),
                conn, params={'t1': start_date, 't2': end_date}
            )
        if not existing.empty:
            existing['key'] = existing['time'].astype(str) + '_' + existing['tripper_name']
            df['key'] = df['time'].astype(str) + '_' + df['tripper_name']
            before = len(df)
            df = df[~df['key'].isin(existing['key'])].drop(columns=['key'])
            print(f"  Skipped existing: {before - len(df)} rows | New rows to write: {len(df)}")
            if df.empty:
                return
        df = df.drop(columns=['key'], errors='ignore')

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
            'tripper_tracking', engine, if_exists='append', index=False, method='multi'
        )
    print(f"  ✅ tripper_tracking write complete ({len(df)} rows)")


# ── Tripper configuration ─────────────────────────────────────────────────────

TRIPPER_CONFIGS = {
    'Tripper1': {
        'CVR111': {'trans_delay': _CVR_DELAYS['CVR111'], 'cvr_prefix': 'cvr1_',
                   'belt_freq_cols': ['cvr12101运行频率', 'cvr12102运行频率', 'cvr14001运行频率']},
        'CVR112': {'trans_delay': _CVR_DELAYS['CVR112'], 'cvr_prefix': 'cvr2_',
                   'belt_freq_cols': ['cvr12101运行频率', 'cvr12102运行频率', 'cvr14001运行频率']},
    },
    'Tripper2': {
        'CVR113': {'trans_delay': _CVR_DELAYS['CVR113'], 'cvr_prefix': 'cvr1_',
                   'belt_freq_cols': ['cvr12201运行频率', 'cvr12202运行频率', 'cvr12203运行频率', 'cvr14002运行频率']},
        'CVR114': {'trans_delay': _CVR_DELAYS['CVR114'], 'cvr_prefix': 'cvr2_',
                   'belt_freq_cols': ['cvr12201运行频率', 'cvr12202运行频率', 'cvr12203运行频率', 'cvr14002运行频率']},
    }
}

# Columns to read from cvr_tracking and carry through to tripper_tracking output
CVR_COLS_TO_CARRY = [
    'instant_total_ore', 'dump_time', 'ore_waste', 'source',
    'fe_concentrate_pct', 'magfe_pct', 'dtr_pct', 'fe_head_pct',
    'imt_p80', 'truck', 'geomet_domain'
]


def load_belt_freq_data(engine, start_date, end_date):
    """Load belt running frequency data from realtime_data."""
    freq_cols = [
        'cvr12101运行频率', 'cvr12102运行频率', 'cvr14001运行频率',
        'cvr12201运行频率', 'cvr12202运行频率', 'cvr12203运行频率', 'cvr14002运行频率'
    ]
    quoted = ', '.join([f'"{c}"' for c in freq_cols])
    query = text(f"""
        SELECT date AS time, {quoted}
        FROM realtime_data
        WHERE date >= :start_date AND date <= :end_date
        ORDER BY date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'start_date': start_date, 'end_date': end_date})
    df['time'] = pd.to_datetime(df['time'])
    return df


def load_cvr_data(engine, cvr_name, start_date, end_date, trans_delay):
    """Load data for a specific CVR from cvr_tracking and apply transport delay."""
    query = text("""
        SELECT time, instant_total_ore, dump_time,
               date, shift, truck, dig_unit, source,
               bench_id, shot_id,
               end_processor_group_reporting, end_processor_group,
               material, truck_payload_t, adjusted_truck_payload_t,
               start_timestamp, end_timestamp,
               stratigraphy, geomet_domain, imt_p80,
               dtr_pct, fe_concentrate_pct, fe_head_pct,
               magfe_pct, magfe_dtr_pct, sio2_concentrate_pct,
               survey_adjusted_factor, cycle_oid, mprs_create_ts
        FROM cvr_tracking
        WHERE cvr_name = :name
          AND time >= :start_date AND time <= :end_date
        ORDER BY time
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'name': cvr_name,
            'start_date': start_date - timedelta(hours=1),
            'end_date': end_date + timedelta(hours=1)
        })
    if df.empty:
        return df
    df['time'] = pd.to_datetime(df['time'])
    # Apply transport delay
    df['adjusted_time'] = df['time'] + pd.Timedelta(minutes=trans_delay)
    return df


def is_valid_belt_frequency(value):
    try:
        if isinstance(value, str):
            value = float(value)
        if isinstance(value, (int, float)) and not np.isnan(value):
            return value > 0
    except (ValueError, TypeError):
        pass
    return False


def is_belt_running(row, belt_freq_cols):
    for col in belt_freq_cols:
        if col not in row.index:
            return pd.NA
        if not is_valid_belt_frequency(row[col]):
            return False
    return True


def process_tripper_for_date(engine, tripper_name, day_start, day_end, belt_data):
    config = TRIPPER_CONFIGS[tripper_name]
    cvr_names = list(config.keys())

    # Load data for each CVR
    cvr_dfs = {}
    for cvr_name, cfg in config.items():
        df = load_cvr_data(engine, cvr_name, day_start, day_end, cfg['trans_delay'])
        if not df.empty:
            cvr_dfs[cvr_name] = df

    time_range = pd.date_range(start=day_start, end=day_end, freq='1min')
    belt_freq_cols = config[cvr_names[0]]['belt_freq_cols']

    rows = []
    for timestamp in time_range:
        row = {'time': timestamp, 'tripper_name': tripper_name,
               'total_ore': pd.NA, 'belt_running': pd.NA,
               'cvr1_instant_ore': pd.NA, 'cvr2_instant_ore': pd.NA,
               'cvr1_dump_time': pd.NA, 'cvr2_dump_time': pd.NA,
               'cvr1_date': pd.NA, 'cvr2_date': pd.NA,
               'cvr1_shift': pd.NA, 'cvr2_shift': pd.NA,
               'cvr1_truck': pd.NA, 'cvr2_truck': pd.NA,
               'cvr1_dig_unit': pd.NA, 'cvr2_dig_unit': pd.NA,
               'cvr1_source': pd.NA, 'cvr2_source': pd.NA,
               'cvr1_bench_id': pd.NA, 'cvr2_bench_id': pd.NA,
               'cvr1_shot_id': pd.NA, 'cvr2_shot_id': pd.NA,
               'cvr1_end_processor_group_reporting': pd.NA, 'cvr2_end_processor_group_reporting': pd.NA,
               'cvr1_end_processor_group': pd.NA, 'cvr2_end_processor_group': pd.NA,
               'cvr1_material': pd.NA, 'cvr2_material': pd.NA,
               'cvr1_truck_payload_t': pd.NA, 'cvr2_truck_payload_t': pd.NA,
               'cvr1_adjusted_truck_payload_t': pd.NA, 'cvr2_adjusted_truck_payload_t': pd.NA,
               'cvr1_start_timestamp': pd.NA, 'cvr2_start_timestamp': pd.NA,
               'cvr1_end_timestamp': pd.NA, 'cvr2_end_timestamp': pd.NA,
               'cvr1_stratigraphy': pd.NA, 'cvr2_stratigraphy': pd.NA,
               'cvr1_geomet_domain': pd.NA, 'cvr2_geomet_domain': pd.NA,
               'cvr1_imt_p80': pd.NA, 'cvr2_imt_p80': pd.NA,
               'cvr1_dtr_pct': pd.NA, 'cvr2_dtr_pct': pd.NA,
               'cvr1_fe_concentrate_pct': pd.NA, 'cvr2_fe_concentrate_pct': pd.NA,
               'cvr1_fe_head_pct': pd.NA, 'cvr2_fe_head_pct': pd.NA,
               'cvr1_magfe_pct': pd.NA, 'cvr2_magfe_pct': pd.NA,
               'cvr1_magfe_dtr_pct': pd.NA, 'cvr2_magfe_dtr_pct': pd.NA,
               'cvr1_sio2_concentrate_pct': pd.NA, 'cvr2_sio2_concentrate_pct': pd.NA,
               'cvr1_survey_adjusted_factor': pd.NA, 'cvr2_survey_adjusted_factor': pd.NA,
               'cvr1_cycle_oid': pd.NA, 'cvr2_cycle_oid': pd.NA,
               'cvr1_mprs_create_ts': pd.NA, 'cvr2_mprs_create_ts': pd.NA}

        # Check whether belt is running
        belt_row = belt_data[belt_data['time'] == timestamp]
        if not belt_row.empty:
            running = is_belt_running(belt_row.iloc[0], belt_freq_cols)
            row['belt_running'] = running

            if running is True:
                ore_values = []
                for cvr_name, cfg in config.items():
                    prefix = cfg['cvr_prefix']  # cvr1_ or cvr2_
                    if cvr_name in cvr_dfs:
                        df = cvr_dfs[cvr_name]
                        match = df[df['adjusted_time'] == timestamp]
                        if not match.empty:
                            r = match.iloc[0]
                            row[f'{prefix}instant_ore'] = r.get('instant_total_ore', pd.NA)
                            row[f'{prefix}dump_time'] = r.get('dump_time', pd.NA)
                            row[f'{prefix}date'] = r.get('date', pd.NA)
                            row[f'{prefix}shift'] = r.get('shift', pd.NA)
                            row[f'{prefix}truck'] = r.get('truck', pd.NA)
                            row[f'{prefix}dig_unit'] = r.get('dig_unit', pd.NA)
                            row[f'{prefix}source'] = r.get('source', pd.NA)
                            row[f'{prefix}bench_id'] = r.get('bench_id', pd.NA)
                            row[f'{prefix}shot_id'] = r.get('shot_id', pd.NA)
                            row[f'{prefix}end_processor_group_reporting'] = r.get('end_processor_group_reporting', pd.NA)
                            row[f'{prefix}end_processor_group'] = r.get('end_processor_group', pd.NA)
                            row[f'{prefix}material'] = r.get('material', pd.NA)
                            row[f'{prefix}truck_payload_t'] = r.get('truck_payload_t', pd.NA)
                            row[f'{prefix}adjusted_truck_payload_t'] = r.get('adjusted_truck_payload_t', pd.NA)
                            row[f'{prefix}start_timestamp'] = r.get('start_timestamp', pd.NA)
                            row[f'{prefix}end_timestamp'] = r.get('end_timestamp', pd.NA)
                            row[f'{prefix}stratigraphy'] = r.get('stratigraphy', pd.NA)
                            row[f'{prefix}geomet_domain'] = r.get('geomet_domain', pd.NA)
                            row[f'{prefix}imt_p80'] = r.get('imt_p80', pd.NA)
                            row[f'{prefix}dtr_pct'] = r.get('dtr_pct', pd.NA)
                            row[f'{prefix}fe_concentrate_pct'] = r.get('fe_concentrate_pct', pd.NA)
                            row[f'{prefix}fe_head_pct'] = r.get('fe_head_pct', pd.NA)
                            row[f'{prefix}magfe_pct'] = r.get('magfe_pct', pd.NA)
                            row[f'{prefix}magfe_dtr_pct'] = r.get('magfe_dtr_pct', pd.NA)
                            row[f'{prefix}sio2_concentrate_pct'] = r.get('sio2_concentrate_pct', pd.NA)
                            row[f'{prefix}survey_adjusted_factor'] = r.get('survey_adjusted_factor', pd.NA)
                            row[f'{prefix}cycle_oid'] = r.get('cycle_oid', pd.NA)
                            row[f'{prefix}mprs_create_ts'] = r.get('mprs_create_ts', pd.NA)
                            val = r.get('instant_total_ore')
                            if pd.notna(val):
                                ore_values.append(float(val))

                # TotalOre = sum of both CVR instant ore
                if ore_values:
                    row['total_ore'] = sum(ore_values)

        rows.append(row)

    return pd.DataFrame(rows)


def process_date_range(start_date, end_date, overwrite=False):
    engine = get_db_engine()
    ensure_schema(engine)

    current_date = start_date
    success_count = 0
    fail_count = 0

    while current_date <= end_date:
        day_start = pd.Timestamp(current_date.date())
        day_end = day_start + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)

        print(f"\n{'=' * 80}")
        print(f"Processing Tripper data for {day_start.date()}")
        print(f"{'=' * 80}")

        try:
            print("Loading belt frequency data...")
            belt_data = load_belt_freq_data(engine, day_start, day_end)
            print(f"  Loaded {len(belt_data)} belt frequency records")

            all_rows = []
            for tripper_name in ['Tripper1', 'Tripper2']:
                print(f"\nProcessing {tripper_name}...")
                df = process_tripper_for_date(engine, tripper_name, day_start, day_end, belt_data)
                print(f"  Generated {len(df)} records for {tripper_name}")
                all_rows.append(df)

            if all_rows:
                final_df = pd.concat(all_rows, ignore_index=True)
                write_to_pg(engine, final_df, overwrite, day_start, day_end)

            print(f"✅ Successfully processed {day_start.date()}")
            success_count += 1

        except Exception as e:
            print(f"❌ Error processing {day_start.date()}: {e}")
            import traceback
            traceback.print_exc()
            fail_count += 1

        current_date += timedelta(days=1)

    engine.dispose()
    print(f"\n{'=' * 80}")
    print(f"✅ Successful: {success_count} days  ❌ Failed: {fail_count} days")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    start_date = datetime(2026, 4,  1, 0, 0, 0)
    end_date   = datetime(2026, 4, 14, 23, 59, 59)
    overwrite  = True
    process_date_range(start_date, end_date, overwrite)
