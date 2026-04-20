"""
2_Stockpile_Generation_PG_Delay.py
Silo tracking — PostgreSQL output version (delay parameters configurable)

Differences from 4_Stockpile_Generation_PG_v2.py:
  - TRIPPER_TO_SILO_DELAY → per-silo t7x values (t7a ~ t7r)
  - SILO_TO_MILL_DELAY    → per-mill-line t8t9t10_x values (a ~ f)
  - Initially all silos/lines share the same values; can be tuned individually after calibration
"""

import os
import pandas as pd
import json
import math
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

DB_CONNECTION = 'postgresql://postgres:postgres@localhost:5432/postgres'

SILOS_COUNT = 18

# ── Delay configuration ───────────────────────────────────────────────────────
_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            '..', 'resources', 'delay.config')

def _load_delay_config():
    try:
        with open(_CONFIG_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️  Failed to read delay.config, using defaults: {e}")
        return {
            "node4_silo":  {f"t7{chr(ord('a')+i)}": 10.0 for i in range(18)},
            "node45_mill": {f"t8t9t10_{chr(ord('a')+i)}": 15.0 for i in range(6)},
        }

_DELAY_CFG = _load_delay_config()

# Silo numbers 1-18 → t7a ~ t7r
_SILO_T7_KEY  = {i: 't7' + chr(ord('a') + i - 1) for i in range(1, 19)}
# Mill numbers 1-6  → t8t9t10_a ~ t8t9t10_f
_MILL_T8T9T10_KEY = {i: 't8t9t10_' + chr(ord('a') + i - 1) for i in range(1, 7)}

def get_silo_delay(silo_num: int) -> float:
    """Tripper → silo delay (T7), looked up by silo number."""
    return _DELAY_CFG['node4_silo'][_SILO_T7_KEY[silo_num]]

def get_mill_delay(mill_num: int) -> float:
    """Silo → mill delay (T8+T9+T10), looked up by mill number."""
    return _DELAY_CFG['node45_mill'][_MILL_T8T9T10_KEY[mill_num]]

# Backwards-compatible constants — some functions still need a single value (silo 1 as representative; all equal initially)
TRIPPER_TO_SILO_DELAY = get_silo_delay(1)   # minutes
SILO_TO_MILL_DELAY    = get_mill_delay(1)   # minutes
TOTAL_DELAY = TRIPPER_TO_SILO_DELAY + SILO_TO_MILL_DELAY

print(f"✅ Delay config loaded | T7(silo 1)={TRIPPER_TO_SILO_DELAY}min | T8+T9+T10(mill 1)={SILO_TO_MILL_DELAY}min")


def get_db_engine():
    engine = create_engine(DB_CONNECTION, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("✅ Database connected successfully")
    return engine


def ensure_schema(engine):
    ddl = """
    CREATE TABLE IF NOT EXISTS silo_tracking (
        time                             TIMESTAMP NOT NULL,
        silo_num                         INTEGER   NOT NULL,
        filling_level                    DOUBLE PRECISION,
        mass                             DOUBLE PRECISION,
        feed_amount                      DOUBLE PRECISION,
        discharge_amount                 DOUBLE PRECISION,
        discharge_composition_tons       JSONB,
        discharge_composition_pct        JSONB,
        composition_number               INTEGER,
        composition_tons                 JSONB,
        composition_pct                  JSONB,
        layers_count                     INTEGER,
        PRIMARY KEY (time, silo_num)
    );
    CREATE INDEX IF NOT EXISTS idx_silo_tracking_time ON silo_tracking (time);
    CREATE INDEX IF NOT EXISTS idx_silo_tracking_num  ON silo_tracking (silo_num);
    """
    migrate = """
    ALTER TABLE IF EXISTS silo_tracking ADD COLUMN IF NOT EXISTS feed_amount         DOUBLE PRECISION;
    ALTER TABLE IF EXISTS silo_tracking ADD COLUMN IF NOT EXISTS composition_number  INTEGER;
    ALTER TABLE IF EXISTS silo_tracking ADD COLUMN IF NOT EXISTS composition_tons    JSONB;
    ALTER TABLE IF EXISTS silo_tracking ADD COLUMN IF NOT EXISTS composition_pct     JSONB;
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.execute(text(migrate))
        conn.commit()
    print("✅ silo_tracking table ready")


def write_to_pg(engine, records, overwrite, start_date, end_date):
    if not records:
        return
    df = pd.DataFrame(records)
    if df.empty:
        return

    if overwrite:
        with engine.connect() as conn:
            conn.execute(text(
                "DELETE FROM silo_tracking WHERE time >= :t1 AND time <= :t2"
            ), {'t1': start_date, 't2': end_date})
            conn.commit()
        print(f"  [overwrite] Deleted existing rows for {start_date} – {end_date}")
    else:
        with engine.connect() as conn:
            existing = pd.read_sql(
                text("SELECT time, silo_num FROM silo_tracking "
                     "WHERE time >= :t1 AND time <= :t2"),
                conn, params={'t1': start_date, 't2': end_date}
            )
        if not existing.empty:
            existing['key'] = existing['time'].astype(str) + '_' + existing['silo_num'].astype(str)
            df['key'] = df['time'].astype(str) + '_' + df['silo_num'].astype(str)
            before = len(df)
            df = df[~df['key'].isin(existing['key'])].drop(columns=['key'])
            print(f"  Skipped existing: {before - len(df)} rows | New rows to write: {len(df)}")
            if df.empty:
                return
        df = df.drop(columns=['key'], errors='ignore')

    # Serialize JSON columns to strings
    json_cols = ['discharge_composition_tons', 'discharge_composition_pct',
                 'composition_tons', 'composition_pct']
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

    chunk = 2000
    for i in range(0, len(df), chunk):
        df.iloc[i:i + chunk].to_sql(
            'silo_tracking', engine, if_exists='append', index=False, method='multi'
        )
    print(f"  ✅ silo_tracking write complete ({len(df)} rows)")


# ── Filling level ↔ mass conversion ──────────────────────────────────────────

def calculate_mass(filling):
    filling_decimal = filling / 100.0
    Bottom = 6
    h = filling_decimal * 9
    a = h / math.tan(math.radians(53))
    TOP = Bottom + 2 * a
    term1 = (TOP / 2) ** 2
    term2 = (TOP / 2) * (Bottom / 2)
    term3 = (Bottom / 2) ** 2
    V = math.pi * h * (term1 + term2 + term3) / 3
    return round(2.5 * V, 2)


def calculate_filling(mass):
    def f(filling_decimal):
        return calculate_mass(filling_decimal * 100) - mass
    low, high = 0.0, 1.0
    if mass <= 0:
        return 0.0
    while high - low > 1e-4:
        mid = (low + high) / 2
        if f(mid) < 0:
            low = mid
        else:
            high = mid
    return round((low + high) / 2 * 100, 2)


class OreLayer:
    def __init__(self, source, data, timestamp):
        self.source = source
        self.data = data
        try:
            self.amount = float(data.get('TotalOre', 0))
        except (ValueError, TypeError):
            self.amount = 0.0
        self.timestamp = timestamp

    def __repr__(self):
        return f"OreLayer(source={self.source}, amount={self.amount:.2f}, time={self.timestamp})"


# ── Data loading (from PostgreSQL) ────────────────────────────────────────────

def load_data_for_date_range(engine, start_date, end_date):
    """Load all required data from PostgreSQL."""
    extended_start = start_date - timedelta(minutes=TOTAL_DELAY + 10)

    # ① Tripper data: from tripper_tracking table
    with engine.connect() as conn:
        all_tripper = pd.read_sql(text("""
            SELECT time AS "Current Timestamp",
                   tripper_name,
                   total_ore AS "TotalOre",
                   belt_running AS "BeltRunning",
                   cvr1_instant_ore, cvr2_instant_ore,
                   cvr1_dump_time, cvr2_dump_time,
                   cvr1_date, cvr2_date,
                   cvr1_shift, cvr2_shift,
                   cvr1_truck, cvr2_truck,
                   cvr1_dig_unit, cvr2_dig_unit,
                   cvr1_source, cvr2_source,
                   cvr1_bench_id, cvr2_bench_id,
                   cvr1_shot_id, cvr2_shot_id,
                   cvr1_ore_waste_block, cvr2_ore_waste_block,
                   cvr1_end_processor_group_reporting, cvr2_end_processor_group_reporting,
                   cvr1_end_processor_group, cvr2_end_processor_group,
                   cvr1_end_processor, cvr2_end_processor,
                   cvr1_material, cvr2_material,
                   cvr1_truck_payload_t, cvr2_truck_payload_t,
                   cvr1_adjusted_truck_payload_t, cvr2_adjusted_truck_payload_t,
                   cvr1_start_timestamp, cvr2_start_timestamp,
                   cvr1_end_timestamp, cvr2_end_timestamp,
                   cvr1_stratigraphy, cvr2_stratigraphy,
                   cvr1_geomet_domain, cvr2_geomet_domain,
                   cvr1_imt_p80, cvr2_imt_p80,
                   cvr1_dtr_pct, cvr2_dtr_pct,
                   cvr1_fe_concentrate_pct, cvr2_fe_concentrate_pct,
                   cvr1_fe_head_pct, cvr2_fe_head_pct,
                   cvr1_magfe_pct, cvr2_magfe_pct,
                   cvr1_magfe_dtr_pct, cvr2_magfe_dtr_pct,
                   cvr1_sio2_concentrate_pct, cvr2_sio2_concentrate_pct,
                   cvr1_survey_adjusted_factor, cvr2_survey_adjusted_factor,
                   cvr1_cycle_oid, cvr2_cycle_oid,
                   cvr1_mprs_create_ts, cvr2_mprs_create_ts
            FROM tripper_tracking
            WHERE time >= :start AND time <= :end
            ORDER BY time
        """), conn, params={'start': extended_start, 'end': end_date})

    all_tripper['Current Timestamp'] = pd.to_datetime(all_tripper['Current Timestamp'])
    tripper1_data = all_tripper[all_tripper['tripper_name'] == 'Tripper1'].drop(columns=['tripper_name']).reset_index(drop=True)
    tripper2_data = all_tripper[all_tripper['tripper_name'] == 'Tripper2'].drop(columns=['tripper_name']).reset_index(drop=True)

    # ② Silo filling levels + tripper car positions + disc feeder frequencies
    silo_cols = [f'n{i}num原矿仓仓位' for i in range(1, 19)]
    tripper_pos_cols = ['n1num布料小车位置', 'n2num布料小车位置']
    disc_cols = [f'n{i}num原矿仓对应圆盘给矿频率' for i in range(1, 19)]
    all_cols = silo_cols + tripper_pos_cols + disc_cols
    quoted = ', '.join([f'"{c}"' for c in all_cols])

    with engine.connect() as conn:
        silo_data = pd.read_sql(text(f"""
            SELECT date AS "时间", {quoted}
            FROM realtime_data
            WHERE date >= :start AND date <= :end
            ORDER BY date
        """), conn, params={'start': extended_start, 'end': end_date})

    silo_data['时间'] = pd.to_datetime(silo_data['时间'])
    silo_data = silo_data.rename(columns={
        **{f'n{i}num原矿仓仓位': f'{i}#原矿仓仓位' for i in range(1, 19)},
        **{f'n{i}num布料小车位置': f'{i}#布料小车位置' for i in range(1, 3)},
        **{f'n{i}num原矿仓对应圆盘给矿频率': f'{i}#圆盘频率' for i in range(1, 19)},
    })

    # ③ Mill throughput
    mill_cols = [f'n{m}num线自磨机处理量t_h' for m in range(1, 7)]
    quoted_mill = ', '.join([f'"{c}"' for c in mill_cols])
    with engine.connect() as conn:
        mill_data = pd.read_sql(text(f"""
            SELECT "时间", {quoted_mill}
            FROM production_lines
            WHERE "时间" >= :start AND "时间" <= :end
            ORDER BY "时间"
        """), conn, params={'start': extended_start, 'end': end_date})

    mill_data = mill_data.rename(columns={
        f'n{m}num线自磨机处理量t_h': f'{m}#线自磨机处理量t/min' for m in range(1, 7)
    })
    for col in [f'{m}#线自磨机处理量t/min' for m in range(1, 7)]:
        if col in mill_data.columns:
            mill_data[col] = pd.to_numeric(mill_data[col], errors='coerce').fillna(0) / 60
    mill_data['时间'] = pd.to_datetime(mill_data['时间'])

    return tripper1_data, tripper2_data, silo_data, mill_data


# ── Processing logic ──────────────────────────────────────────────────────────

def get_initial_silo_levels(silo_data, start_date):
    initial_levels = {}
    initial_time = start_date - timedelta(minutes=TOTAL_DELAY + 10)
    nearby_data = silo_data[
        (silo_data['时间'] >= initial_time) &
        (silo_data['时间'] <= start_date)
    ]
    for i in range(1, SILOS_COUNT + 1):
        col = f'{i}#原矿仓仓位'
        if col in nearby_data.columns and not nearby_data.empty:
            val = pd.to_numeric(nearby_data[col].iloc[0], errors='coerce')
            initial_levels[i] = float(val) if pd.notna(val) else 50.0
        else:
            initial_levels[i] = 50.0
    return initial_levels


def initialize_silos(initial_levels):
    silos = {}
    for i in range(1, SILOS_COUNT + 1):
        filling = initial_levels.get(i, 50.0)
        mass = calculate_mass(filling)
        initial_layer = OreLayer(
            source='Unknown',
            data={'TotalOre': float(mass)},
            timestamp=datetime.min
        )
        silos[i] = {'filling': filling, 'mass': mass, 'layers': [initial_layer]}
    return silos


def get_silo_being_filled(silo_data, current_time, tripper_number):
    """Find the silo number currently being filled by the tripper, from realtime_data (silo_data)."""
    delayed_time = current_time - timedelta(minutes=TRIPPER_TO_SILO_DELAY)
    row = silo_data[silo_data['时间'] == delayed_time]
    if row.empty:
        # Fall back to the most recent row before delayed_time
        earlier = silo_data[silo_data['时间'] <= delayed_time]
        if earlier.empty:
            return None
        row = silo_data[silo_data['时间'] == earlier['时间'].max()]
    if row.empty:
        return None

    col = f'{tripper_number}#布料小车位置'
    if col not in row.columns:
        return None
    position = row[col].iloc[0]
    if pd.isna(position):
        return None

    try:
        silo_num = int(round(float(position)))
    except (ValueError, TypeError):
        return None

    # Tripper1 → silos 1-9, Tripper2 → silos 10-18
    if tripper_number == 1 and 1 <= silo_num <= 9:
        return silo_num
    elif tripper_number == 2 and 10 <= silo_num <= 18:
        return silo_num
    return None


def get_tripper_feed_rate(tripper_data, current_time):
    delayed_time = current_time - timedelta(minutes=TRIPPER_TO_SILO_DELAY)
    row = tripper_data[tripper_data['Current Timestamp'] == delayed_time]
    if row.empty or 'TotalOre' not in row.columns:
        return 0, {}

    total_ore = row['TotalOre'].iloc[0]
    try:
        total_ore = float(total_ore) if pd.notna(total_ore) else 0
    except (ValueError, TypeError):
        total_ore = 0

    # Extract ore source info for layer tracking
    # If source is NULL, fall back to truck; if that is also NULL, use a cvr-prefix placeholder
    source_info = {}
    for prefix in ['cvr1_', 'cvr2_']:
        source_col = f'{prefix}source'
        truck_col  = f'{prefix}truck'
        ore_col    = f'{prefix}instant_ore'
        if ore_col not in row.columns:
            continue
        ore = row[ore_col].iloc[0]
        if not pd.notna(ore):
            continue
        try:
            ore_val = float(ore)
        except (ValueError, TypeError):
            continue
        if ore_val <= 0:
            continue
        # Determine source identifier (source > truck > placeholder)
        source = row[source_col].iloc[0] if source_col in row.columns else None
        if not pd.notna(source):
            source = row[truck_col].iloc[0] if truck_col in row.columns else None
        if not pd.notna(source):
            source = f'{prefix}unknown'
        source_info[str(source)] = source_info.get(str(source), 0) + ore_val

    return total_ore, source_info


def get_mill_processing_rate(mill_data, current_time, mill_number):
    row = mill_data[mill_data['时间'] == current_time]
    if row.empty:
        return 0
    col = f'{mill_number}#线自磨机处理量t/min'
    if col not in row.columns:
        return 0
    val = row[col].iloc[0]
    try:
        return float(val) if pd.notna(val) else 0
    except (ValueError, TypeError):
        return 0


def get_disc_freqs(silo_data, current_time, silo_numbers):
    row = silo_data[silo_data['时间'] == current_time]
    if row.empty:
        window = silo_data[
            (silo_data['时间'] >= current_time - timedelta(minutes=1)) &
            (silo_data['时间'] <= current_time + timedelta(minutes=1))
        ]
        if not window.empty:
            closest = (window['时间'] - current_time).abs().idxmin()
            row = silo_data.loc[[closest]]

    freqs = {}
    for silo_num in silo_numbers:
        col = f'{silo_num}#圆盘频率'
        if not row.empty and col in row.columns:
            val = pd.to_numeric(row[col].iloc[0], errors='coerce')
            freqs[silo_num] = float(val) if pd.notna(val) else 0.0
        else:
            freqs[silo_num] = 0.0
    return freqs


MILL_TO_SILOS = {1: [1, 2, 3], 2: [4, 5, 6], 3: [7, 8, 9],
                 4: [10, 11, 12], 5: [13, 14, 15], 6: [16, 17, 18]}


def update_silo(silo, feed_amount, feed_data, discharge_amount, discharge_data):
    if feed_amount > 0 and feed_data:
        # Each source gets its own layer
        for source, ore_val in feed_data.items():
            if ore_val > 0:
                silo['layers'].append(OreLayer(
                    source=str(source),
                    data={'TotalOre': float(ore_val)},
                    timestamp=datetime.now()
                ))
        silo['mass'] += feed_amount

    if discharge_amount > 0 and silo['mass'] > 0:
        discharge_ratio = min(discharge_amount / silo['mass'], 1.0)
        discharge_composition = {}

        remaining_layers = []
        for layer in silo['layers']:
            layer_discharge = layer.amount * discharge_ratio
            if layer.source not in discharge_composition:
                discharge_composition[layer.source] = 0
            discharge_composition[layer.source] += layer_discharge
            layer.amount -= layer_discharge
            if layer.amount > 0.01:
                remaining_layers.append(layer)

        silo['layers'] = remaining_layers
        silo['mass'] -= discharge_amount
        silo['mass'] = max(0, silo['mass'])
    else:
        discharge_composition = {}

    silo['filling'] = calculate_filling(silo['mass'])

    # Current silo composition (remaining ore layers after discharge)
    composition_tons = {}
    for layer in silo['layers']:
        composition_tons[layer.source] = composition_tons.get(layer.source, 0) + float(layer.amount)
    composition_number = len(composition_tons)
    total_comp = sum(composition_tons.values())
    composition_pct = (
        {k: v / total_comp * 100 for k, v in composition_tons.items()}
        if total_comp > 0 else {}
    )

    return discharge_composition, composition_tons, composition_pct, composition_number


def process_time_step(current_time, silos, tripper1_data, tripper2_data,
                      silo_data, mill_data):
    records = []

    for mill_num, silo_nums in MILL_TO_SILOS.items():
        mill_time = current_time + timedelta(minutes=get_mill_delay(mill_num))
        mill_rate = get_mill_processing_rate(mill_data, mill_time, mill_num)
        disc_freqs = get_disc_freqs(silo_data, current_time, silo_nums)
        total_freq = sum(disc_freqs.values())

        for silo_num in silo_nums:
            tripper_num = 1 if silo_num <= 9 else 2
            curr_tripper_data = tripper1_data if tripper_num == 1 else tripper2_data

            feed_amount, feed_data = get_tripper_feed_rate(curr_tripper_data, current_time)
            silo_being_filled = get_silo_being_filled(silo_data, current_time, tripper_num)
            actual_feed = feed_amount if silo_being_filled == silo_num else 0
            actual_feed_data = feed_data if silo_being_filled == silo_num else {}

            if total_freq > 0:
                discharge_amount = mill_rate * (disc_freqs[silo_num] / total_freq)
            else:
                discharge_amount = 0

            discharge_composition, composition_tons, composition_pct, composition_number = update_silo(
                silos[silo_num], actual_feed, actual_feed_data, discharge_amount, {}
            )

            disc_pct = {}
            total_disc = sum(discharge_composition.values())
            if total_disc > 0:
                disc_pct = {k: v / total_disc * 100 for k, v in discharge_composition.items()}

            records.append({
                'time': current_time,
                'silo_num': silo_num,
                'filling_level': silos[silo_num]['filling'],
                'mass': silos[silo_num]['mass'],
                'feed_amount': actual_feed,
                'discharge_amount': discharge_amount,
                'discharge_composition_tons': discharge_composition,
                'discharge_composition_pct': disc_pct,
                'composition_number': composition_number,
                'composition_tons': composition_tons,
                'composition_pct': composition_pct,
                'layers_count': len(silos[silo_num]['layers']),
            })

    return records


def generate_silo_tracking_data(start_date, end_date, overwrite=False):
    engine = get_db_engine()
    ensure_schema(engine)

    print(f"\nLoading data {start_date.date()} ~ {end_date.date()}...")
    tripper1_data, tripper2_data, silo_data, mill_data = load_data_for_date_range(
        engine, start_date, end_date
    )

    print(f"Tripper1: {len(tripper1_data)} rows  Tripper2: {len(tripper2_data)} rows")
    print(f"Silo data: {len(silo_data)} rows  Mill data: {len(mill_data)} rows")

    initial_levels = get_initial_silo_levels(silo_data, start_date)
    silos = initialize_silos(initial_levels)

    # Process day by day
    current_date = start_date
    while current_date <= end_date:
        day_start = pd.Timestamp(current_date.date())
        day_end = day_start + pd.Timedelta(days=1) - pd.Timedelta(minutes=1)
        time_range = pd.date_range(start=day_start, end=day_end, freq='1min')

        print(f"\n{'=' * 60}")
        print(f"Processing {day_start.date()}  ({len(time_range)} minutes)")
        print(f"{'=' * 60}")

        day_records = []
        for i, current_time in enumerate(time_range):
            records = process_time_step(
                current_time, silos,
                tripper1_data, tripper2_data,
                silo_data, mill_data
            )
            day_records.extend(records)

            if (i + 1) % 200 == 0:
                print(f"  Progress: {i + 1}/{len(time_range)}")

        write_to_pg(engine, day_records, overwrite, day_start, day_end)
        current_date += timedelta(days=1)

    engine.dispose()
    print("\n✅ silo_tracking generation complete!")


if __name__ == "__main__":
    start_date = datetime(2025, 4,  1, 0, 0, 0)
    end_date   = datetime(2025, 4, 14, 23, 59, 59)
    overwrite  = True
    generate_silo_tracking_data(start_date, end_date, overwrite)
