"""
Import_PI_Minestar_to_PG_v1-1.py
Import PI Web API data and Minestar truck-cycle data directly into PostgreSQL
without intermediate CSV files.

Import targets:
  ① PI → realtime_data   : 18 silo levels + 2 tripper positions + 18 disc-feeder frequencies
  ① PI → production_lines: 6-line SAG mill throughput
  ② Minestar → truck_cycles: truck cycle records

Connection config:
  PI Web API : https://pivision.cpmining.local/piwebapi  (Windows Integrated Auth)
  Minestar   : DSN=sino  (ODBC — must be configured on the workstation)
  PostgreSQL : postgresql://postgres:postgres@localhost:5432/postgres

overwrite parameter (default False):
  False → skip records that already exist in the target time window (incremental append, safe)
  True  → delete existing records in the window first, then write all (full overwrite)
"""

import requests
from requests_negotiate_sspi import HttpNegotiateAuth
import urllib3
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import urllib.parse
import json
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Configuration ────────────────────────────────────────────────────────────
PI_BASE_URL      = "https://pivision.cpmining.local/piwebapi"
DATASERVER       = "mes"
PI_INTERVAL      = "1m"
WEBID_CACHE_FILE = "./data/PI_TAG_WEBID_MAP_mes.json"

DB_CONNECTION    = 'postgresql://postgres:postgres@localhost:5432/postgres'

MINESTAR_DSN      = "sino"
MINESTAR_USER     = "dw_processing_Ore_blending_model"
MINESTAR_PASSWORD = "gupEstuzayacep7"
MINESTAR_TABLE    = "[CPM_Datawarehouse].[Processing_Ore_blending_model].[TruckCycle]"

# ── PI Tag → (table, column) mapping ─────────────────────────────────────────
PI_TAG_MAPPING = {
    # Raw-ore silo levels (18 silos)
    '1_LIT140121A_V_PV': ('realtime_data', 'n1num原矿仓仓位'),
    '1_LIT140121B_V_PV': ('realtime_data', 'n2num原矿仓仓位'),
    '1_LIT140121C_V_PV': ('realtime_data', 'n3num原矿仓仓位'),
    '1_LIT140122A_V_PV': ('realtime_data', 'n4num原矿仓仓位'),
    '1_LIT140122B_V_PV': ('realtime_data', 'n5num原矿仓仓位'),
    '1_LIT140122C_V_PV': ('realtime_data', 'n6num原矿仓仓位'),
    '1_LI140123A_V_PV':  ('realtime_data', 'n7num原矿仓仓位'),
    '1_LI140123B_V_PV':  ('realtime_data', 'n8num原矿仓仓位'),
    '1_LI140123C_V_PV':  ('realtime_data', 'n9num原矿仓仓位'),
    '1_LI140124A_V_PV':  ('realtime_data', 'n10num原矿仓仓位'),
    '1_LI140124B_V_PV':  ('realtime_data', 'n11num原矿仓仓位'),
    '1_LI140124C_V_PV':  ('realtime_data', 'n12num原矿仓仓位'),
    '1_LI140125A_V_PV':  ('realtime_data', 'n13num原矿仓仓位'),
    '1_LI140125B_V_PV':  ('realtime_data', 'n14num原矿仓仓位'),
    '1_LI140125C_V_PV':  ('realtime_data', 'n15num原矿仓仓位'),
    '1_LI140126A_V_PV':  ('realtime_data', 'n16num原矿仓仓位'),
    '1_LI140126B_V_PV':  ('realtime_data', 'n17num原矿仓仓位'),
    '1_LI140126C_V_PV':  ('realtime_data', 'n18num原矿仓仓位'),
    # Tripper car position + speed (2 each)
    '1_TRP14001_V_BIN':          ('realtime_data', 'n1num布料小车位置'),
    '1_TRP14002_V_BIN':          ('realtime_data', 'n2num布料小车位置'),
    '1_TRP14001_V_M1_VSD_Speed': ('realtime_data', 'n1num布料小车速度'),
    '1_TRP14002_V_M1_VSD_Speed': ('realtime_data', 'n2num布料小车速度'),
    # Long-belt running frequency (7 belts — used to detect belt running state)
    '1_CVR12101MTR1_V_SPD': ('realtime_data', 'cvr12101运行频率'),
    '1_CVR12102MTR1_V_SPD': ('realtime_data', 'cvr12102运行频率'),
    '1_CVR14001MTR_V_SPD':  ('realtime_data', 'cvr14001运行频率'),
    '1_CVR12201MTR_V_SPD':  ('realtime_data', 'cvr12201运行频率'),
    '1_CVR12202MTR1_V_SPD': ('realtime_data', 'cvr12202运行频率'),
    '1_CVR12203MTR1_V_SPD': ('realtime_data', 'cvr12203运行频率'),
    '1_CVR14002MTR_V_SPD':  ('realtime_data', 'cvr14002运行频率'),
    # Crusher feed bin level (4 crushers × 2 sensors)
    '1_CRU11101BIN_V_INP1': ('realtime_data', 'n1num旋回破碎机给料仓料位'),
    '1_CRU11101BIN_V_INP2': ('realtime_data', 'n1num旋回破碎机给料仓料位_1'),
    '1_CRU11201BIN_V_INP1': ('realtime_data', 'n2num旋回破碎机给料仓料位'),
    '1_CRU11201BIN_V_INP2': ('realtime_data', 'n2num旋回破碎机给料仓料位_1'),
    '1_CRU11301BIN_V_INP1': ('realtime_data', 'n3num旋回破碎机给料仓料位'),
    '1_CRU11301BIN_V_INP2': ('realtime_data', 'n3num旋回破碎机给料仓料位_1'),
    '1_CRU11401BIN_V_INP1': ('realtime_data', 'n4num旋回破碎机给料仓料位'),
    '1_CRU11401BIN_V_INP2': ('realtime_data', 'n4num旋回破碎机给料仓料位_1'),
    # Crusher running power (4 crushers)
    '1_CRU11101GYR_V_KW':   ('realtime_data', 'n1num旋回破碎机运行功率'),
    '1_CRU11201GYR_V_KW':   ('realtime_data', 'n2num旋回破碎机运行功率'),
    '1_CRU11301GYR_V_KW':   ('realtime_data', 'n3num旋回破碎机运行功率'),
    '1_CRU11401GYR_V_KW':   ('realtime_data', 'n4num旋回破碎机运行功率'),
    # Crusher buffer bin level (4 crushers)
    '1_CRU11101BIN_V_OUT1': ('realtime_data', 'n1num旋回破碎机缓冲仓料位'),
    '1_CRU11201BIN_V_OUT1': ('realtime_data', 'n2num旋回破碎机缓冲仓料位'),
    '1_CRU11301BIN_V_OUT1': ('realtime_data', 'n3num旋回破碎机缓冲仓料位'),
    '1_CRU11401BIN_V_OUT1': ('realtime_data', 'n4num旋回破碎机缓冲仓料位'),
    # Discharge belt ore tonnage (4 belts — used directly by crusher script)
    '1_CVR11101BSCALE_V_PV': ('realtime_data', 'n1num排料皮带矿量'),
    '1_CVR11201BSCALE_V_PV': ('realtime_data', 'n2num排料皮带矿量'),
    '1_CVR11301BSCALE_V_PV': ('realtime_data', 'n3num排料皮带矿量'),
    '1_CVR11401BSCALE_V_PV': ('realtime_data', 'n4num排料皮带矿量'),
    # Discharge belt frequency (4 belts)
    '1_CVR11101MTR_V_SPD':   ('realtime_data', 'n1num排料皮带频率'),
    '1_CVR11201MTR_V_SPD':   ('realtime_data', 'n2num排料皮带频率'),
    '1_CVR11301MTR_V_SPD':   ('realtime_data', 'n3num排料皮带频率'),
    '1_CVR11401MTR_V_SPD':   ('realtime_data', 'n4num排料皮带频率'),
    # Average silo level / average ore tonnage (aggregate indicators)
    '1_LI14012XXAVG_V_PV':   ('realtime_data', '平均仓位'),
    '2_WI21X1134AVG_V_PV':   ('realtime_data', '平均矿量'),
    # Disc feeder frequencies (18 silos)
    '2_WFY2111134A_V_CV': ('realtime_data', 'n1num原矿仓对应圆盘给矿频率'),
    '2_WFY2111134B_V_CV': ('realtime_data', 'n2num原矿仓对应圆盘给矿频率'),
    '2_WFY2111134C_V_CV': ('realtime_data', 'n3num原矿仓对应圆盘给矿频率'),
    '2_WFY2121134A_V_CV': ('realtime_data', 'n4num原矿仓对应圆盘给矿频率'),
    '2_WFY2121134B_V_CV': ('realtime_data', 'n5num原矿仓对应圆盘给矿频率'),
    '2_WFY2121134C_V_CV': ('realtime_data', 'n6num原矿仓对应圆盘给矿频率'),
    '2_AFD21301MTR_V_SPD': ('realtime_data', 'n7num原矿仓对应圆盘给矿频率'),
    '2_AFD21302MTR_V_SPD': ('realtime_data', 'n8num原矿仓对应圆盘给矿频率'),
    '2_AFD21303MTR_V_SPD': ('realtime_data', 'n9num原矿仓对应圆盘给矿频率'),
    '2_AFD21401MTR_V_SPD': ('realtime_data', 'n10num原矿仓对应圆盘给矿频率'),
    '2_AFD21402MTR_V_SPD': ('realtime_data', 'n11num原矿仓对应圆盘给矿频率'),
    '2_AFD21403MTR_V_SPD': ('realtime_data', 'n12num原矿仓对应圆盘给矿频率'),
    '2_AFD21501MTR_V_SPD': ('realtime_data', 'n13num原矿仓对应圆盘给矿频率'),
    '2_AFD21502MTR_V_SPD': ('realtime_data', 'n14num原矿仓对应圆盘给矿频率'),
    '2_AFD21503MTR_V_SPD': ('realtime_data', 'n15num原矿仓对应圆盘给矿频率'),
    '2_AFD21601MTR_V_SPD': ('realtime_data', 'n16num原矿仓对应圆盘给矿频率'),
    '2_AFD21602MTR_V_SPD': ('realtime_data', 'n17num原矿仓对应圆盘给矿频率'),
    '2_AFD21603MTR_V_SPD': ('realtime_data', 'n18num原矿仓对应圆盘给矿频率'),
    # SAG mill throughput (6 lines)
    '2_WIT2111134_V_PV':  ('production_lines', 'n1num线自磨机处理量t_h'),
    '2_WIT2121134_V_PV':  ('production_lines', 'n2num线自磨机处理量t_h'),
    '2_WI2131134_V_PV':   ('production_lines', 'n3num线自磨机处理量t_h'),
    '2_WI2141134_V_PV':   ('production_lines', 'n4num线自磨机处理量t_h'),
    '2_WI2151134_V_PV':   ('production_lines', 'n5num线自磨机处理量t_h'),
    '2_WI2161134_V_PV':   ('production_lines', 'n6num线自磨机处理量t_h'),
    # SAG mill running power (6 lines)
    '2_JI2111144_V_PV':   ('production_lines', 'n1num自磨机运行功率'),
    '2_JI2121144_V_PV':   ('production_lines', 'n2num自磨机运行功率'),
    '2_JI2131144_V_PV':   ('production_lines', 'n3num自磨机运行功率'),
    '2_JI2141144_V_PV':   ('production_lines', 'n4num自磨机运行功率'),
    '2_JI2151144_V_PV':   ('production_lines', 'n5num自磨机运行功率'),
    '2_JI2161144_V_PV':   ('production_lines', 'n6num自磨机运行功率'),
    # SAG mill running current (6 lines)
    '2_AGM21101MTR_V_AMP': ('production_lines', 'n1num自磨机运行电流'),
    '2_AGM21201MTR_V_AMP': ('production_lines', 'n2num自磨机运行电流'),
    '2_II2131144_V_PV':    ('production_lines', 'n3num自磨机运行电流'),
    '2_II2141144_V_PV':    ('production_lines', 'n4num自磨机运行电流'),
    '2_II2151144_V_PV':    ('production_lines', 'n5num自磨机运行电流'),
    '2_II2161144_V_PV':    ('production_lines', 'n6num自磨机运行电流'),
    # SAG mill rotation speed (lines 1-2: motor speed tag; lines 3-6: tachometer tag)
    '2_AGM21101MTR_V_RT01': ('production_lines', 'n1num自磨机转速'),
    '2_AGM21201MTR_V_RT01': ('production_lines', 'n2num自磨机转速'),
    '2_AGM21301_V_SPD':     ('production_lines', 'n3num自磨机转速'),
    '2_SI2141145_V_PV':     ('production_lines', 'n4num自磨机转速'),
    '2_SI2151145_V_PV':     ('production_lines', 'n5num自磨机转速'),
    '2_SI2161145_V_PV':     ('production_lines', 'n6num自磨机转速'),
    # SAG mill feed F80 value (6 lines)
    '2_Line1_OCS_F80':  ('production_lines', 'n1num自磨机给矿f80值'),
    '2_Line2_OCS_F80':  ('production_lines', 'n2num自磨机给矿f80值'),
    '2_Line3_OCS_F80':  ('production_lines', 'n3num自磨机给矿f80值'),
    '2_Line4_OCS_F80':  ('production_lines', 'n4num自磨机给矿f80值'),
    '2_Line5_OCS_F80':  ('production_lines', 'n5num自磨机给矿f80值'),
    '2_Line6_OCS_F80':  ('production_lines', 'n6num自磨机给矿f80值'),
    # SAG mill pebble discharge (6 lines)
    '2_WIT2111178_V_PV': ('production_lines', 'n1num自磨顽石排出量'),
    '2_WIT2121178_V_PV': ('production_lines', 'n2num自磨顽石排出量'),
    '2_WI2131178_V_PV':  ('production_lines', 'n3num自磨顽石排出量'),
    '2_WI2141178_V_PV':  ('production_lines', 'n4num自磨顽石排出量'),
    '2_WI2151178_V_PV':  ('production_lines', 'n5num自磨顽石排出量'),
    '2_WI2161178_V_PV':  ('production_lines', 'n6num自磨顽石排出量'),
}

TABLE_TIME_COL = {
    'realtime_data':    'date',
    'production_lines': '时间',
}

# Minestar column name → PG truck_cycles column name
# Naming convention: matches legacy CSV-import PG column names to minimise downstream changes
MINESTAR_COL_RENAME = {
    # Ore quality indicators (% suffix)
    'fe_concentrate':   'fe_concentrate_pct',
    'magfe_dtr':        'magfe_dtr_pct',
    'dtr':              'dtr_pct',
    'fe_head':          'fe_head_pct',
    'sio2_concentrate': 'sio2_concentrate_pct',
    'mag_fe':           'magfe_pct',
    # Geology / location fields
    'geomat_domain':    'geomet_domain',        # legacy: Geomet Domain → geomet_domain
    'bench_name':       'bench_id',             # Minestar exports bench_name; PG column is bench_id
    'end_processor':    'end_processor_group_reporting',  # e.g. CR1, CR2 → reporting destination
    # 'material'   → 'ore_waste': deprecated — truck_cycles stores material; rename would lose data
    'shot_name':        'shot_id',              # legacy: Shot ID → shot_id (now a string e.g. WST:0838:0146)
    # Minestar system timestamp (prefixed to distinguish from business timestamps)
    'CreateTimestamp':  'mprs_create_ts',
}
# Columns from Minestar that are NOT written to PG
# - CreatedBy: no business value
# - Cycle_date: Minestar internal date; we derive date ourselves from start_timestamp
# - Ore_Waste_Block: does not exist in the Minestar source view (absent from SELECT *)
MINESTAR_DROP_COLS = ['CreatedBy', 'Cycle_date', 'Ore_Waste_Block']


# ══════════════════════════════════════════════════════════════════════════════
# PI Importer
# ══════════════════════════════════════════════════════════════════════════════

class PIImporter:
    def __init__(self, pg_engine):
        self.session = requests.Session()
        self.session.auth = HttpNegotiateAuth()
        self.session.verify = False
        self.engine = pg_engine
        self._webid_cache = self._load_webid_cache()
        self._ds_webid = None

    # ── WebID cache ───────────────────────────────────────────────────────────

    def _load_webid_cache(self):
        cache = {}
        if os.path.exists(WEBID_CACHE_FILE):
            try:
                with open(WEBID_CACHE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                for item in data:
                    if item.get('status') == 'OK' and item.get('webid') and item.get('tag'):
                        cache[item['tag']] = item['webid']
                print(f"WebID cache loaded: {len(cache)} tags")
            except Exception as e:
                print(f"Failed to read WebID cache: {e}")
        else:
            print(f"WebID cache not found — will query PI API live ({WEBID_CACHE_FILE})")
        return cache

    def _save_webid_cache(self):
        os.makedirs(os.path.dirname(WEBID_CACHE_FILE) or '.', exist_ok=True)
        entries = [{'tag': t, 'webid': w, 'status': 'OK', 'error': ''}
                   for t, w in self._webid_cache.items()]
        try:
            with open(WEBID_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(entries, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Failed to save WebID cache: {e}")

    # ── PI requests ───────────────────────────────────────────────────────────

    def _get(self, endpoint, params=None):
        url = f"{PI_BASE_URL}/{endpoint.lstrip('/')}"
        try:
            r = self.session.get(url, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"  PI request failed [{endpoint}]: {e}")
            return None

    def _get_ds_webid(self):
        if self._ds_webid:
            return self._ds_webid
        data = self._get("dataservers")
        if not data:
            return None
        for s in data.get('Items', []):
            if s.get('Name') == DATASERVER:
                self._ds_webid = s['WebId']
                print(f"Connected to DataServer: {DATASERVER}")
                return self._ds_webid
        print(f"DataServer not found: {DATASERVER}")
        return None

    def _resolve_webid(self, tag):
        if tag in self._webid_cache:
            return self._webid_cache[tag]
        ds_webid = self._get_ds_webid()
        if not ds_webid:
            return None
        data = self._get(f"dataservers/{ds_webid}/points",
                         params={'nameFilter': tag, 'maxCount': 25})
        if data and 'Items' in data:
            for item in data['Items']:
                if item.get('Name') == tag:
                    webid = item['WebId']
                    self._webid_cache[tag] = webid
                    return webid
        print(f"  Tag not found: {tag}")
        return None

    def _query_interpolated(self, tag, start_dt, end_dt):
        webid = self._resolve_webid(tag)
        if not webid:
            return pd.DataFrame(columns=['ts', 'value'])

        # Auto-chunk: PI interpolated endpoint has a ~150k point limit per request.
        # At 1-min interval, 60 days ≈ 86,400 points — safely within limit.
        CHUNK_DAYS = 60
        total_days = (end_dt - start_dt).days
        if total_days > CHUNK_DAYS:
            chunks = []
            chunk_start = start_dt
            while chunk_start < end_dt:
                chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), end_dt)
                chunk_df = self._query_interpolated_single(tag, webid, chunk_start, chunk_end)
                if not chunk_df.empty:
                    chunks.append(chunk_df)
                chunk_start = chunk_end
            return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame(columns=['ts', 'value'])

        return self._query_interpolated_single(tag, webid, start_dt, end_dt)

    def _query_interpolated_single(self, tag, webid, start_dt, end_dt):
        params = {
            'startTime': start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'endTime':   end_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'interval':  PI_INTERVAL,
        }
        data = self._get(f"streams/{webid}/interpolated", params=params)
        if not data or 'Items' not in data:
            return pd.DataFrame(columns=['ts', 'value'])
        rows = []
        for item in data['Items']:
            val = item.get('Value')
            if isinstance(val, dict):   # PI system status value — skip
                continue
            try:
                ts = pd.to_datetime(item['Timestamp']).tz_localize(None)
                rows.append({'ts': ts, 'value': float(val)})
            except Exception:
                continue
        return pd.DataFrame(rows)

    # ── Write to PG ───────────────────────────────────────────────────────────

    def _ensure_schema(self):
        """Ensure all new columns exist in realtime_data."""
        new_cols = (
            [f'n{i}num原矿仓对应圆盘给矿频率' for i in range(1, 19)] +
            ['cvr12101运行频率', 'cvr12102运行频率', 'cvr14001运行频率',
             'cvr12201运行频率', 'cvr12202运行频率', 'cvr12203运行频率', 'cvr14002运行频率',
             'n1num布料小车速度', 'n2num布料小车速度',
             'n1num旋回破碎机给料仓料位', 'n1num旋回破碎机给料仓料位_1',
             'n2num旋回破碎机给料仓料位', 'n2num旋回破碎机给料仓料位_1',
             'n3num旋回破碎机给料仓料位', 'n3num旋回破碎机给料仓料位_1',
             'n4num旋回破碎机给料仓料位', 'n4num旋回破碎机给料仓料位_1',
             'n1num旋回破碎机运行功率', 'n2num旋回破碎机运行功率',
             'n3num旋回破碎机运行功率', 'n4num旋回破碎机运行功率',
             'n1num旋回破碎机缓冲仓料位', 'n2num旋回破碎机缓冲仓料位',
             'n3num旋回破碎机缓冲仓料位', 'n4num旋回破碎机缓冲仓料位',
             'n1num排料皮带矿量', 'n2num排料皮带矿量',
             'n3num排料皮带矿量', 'n4num排料皮带矿量',
             'n1num排料皮带频率', 'n2num排料皮带频率',
             'n3num排料皮带频率', 'n4num排料皮带频率',
             '平均仓位', '平均矿量']
        )
        with self.engine.connect() as conn:
            for col in new_cols:
                try:
                    conn.execute(text(
                        f'ALTER TABLE realtime_data ADD COLUMN IF NOT EXISTS "{col}" double precision'
                    ))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    if 'already exists' not in str(e):
                        print(f"  Warning adding column {col}: {e}")

        # production_lines columns added over time
        prod_new_cols = ['n2num自磨机转速']
        with self.engine.connect() as conn:
            for col in prod_new_cols:
                try:
                    conn.execute(text(
                        f'ALTER TABLE production_lines ADD COLUMN IF NOT EXISTS "{col}" double precision'
                    ))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    if 'already exists' not in str(e):
                        print(f"  Warning adding column {col}: {e}")

    def _write_to_pg(self, df, table, time_col, overwrite):
        t_min, t_max = df[time_col].min(), df[time_col].max()
        if overwrite:
            with self.engine.connect() as conn:
                conn.execute(text(
                    f'DELETE FROM {table} WHERE "{time_col}" >= :t1 AND "{time_col}" <= :t2'
                ), {'t1': t_min, 't2': t_max})
                conn.commit()
            print(f"  [overwrite] Deleted existing rows in {table} for {t_min} ~ {t_max}")
        else:
            with self.engine.connect() as conn:
                existing_df = pd.read_sql(
                    text(f'SELECT "{time_col}" FROM {table} '
                         f'WHERE "{time_col}" >= :t1 AND "{time_col}" <= :t2'),
                    conn, params={'t1': t_min, 't2': t_max}
                )
            existing_ts = set(pd.to_datetime(existing_df[time_col]).dt.tz_localize(None))
            before = len(df)
            df = df[~df[time_col].isin(existing_ts)].copy()
            print(f"  Skipped existing: {before - len(df)} rows | New rows to write: {len(df)}")
            if df.empty:
                print(f"  ℹ️  {table}: all data for this period already exists")
                return

        chunk = 500
        for i in range(0, len(df), chunk):
            df.iloc[i:i + chunk].to_sql(
                table, self.engine, if_exists='append', index=False, method='multi'
            )
        print(f"  ✅ {table} write complete ({len(df)} rows)")

    # ── Main entry ────────────────────────────────────────────────────────────

    def run(self, start_date, end_date, overwrite=False):
        print(f"\n{'─' * 60}")
        print(f"[PI] Starting import  {start_date.date()} ~ {end_date.date()}")
        print(f"     overwrite={overwrite} | tags={len(PI_TAG_MAPPING)}")
        print(f"{'─' * 60}")

        self._ensure_schema()

        table_cols = {'realtime_data': {}, 'production_lines': {}}

        for idx, (tag, (table, col)) in enumerate(PI_TAG_MAPPING.items(), 1):
            print(f"[{idx:02d}/{len(PI_TAG_MAPPING)}] {tag}")
            df = self._query_interpolated(tag, start_date, end_date)
            if df.empty:
                print(f"  ⚠️  No data returned")
                continue
            print(f"  ✓ {len(df)} rows")
            table_cols[table][col] = df

        self._save_webid_cache()

        for table, col_dict in table_cols.items():
            if not col_dict:
                continue
            time_col = TABLE_TIME_COL[table]
            print(f"\nMerging {table} ({len(col_dict)} columns)...")
            merged = None
            for col, df in col_dict.items():
                df_r = df.rename(columns={'ts': time_col, 'value': col})
                merged = df_r if merged is None else pd.merge(merged, df_r, on=time_col, how='outer')
            merged = merged.sort_values(time_col).reset_index(drop=True)
            merged[time_col] = pd.to_datetime(merged[time_col])
            num_cols = merged.select_dtypes(include=[np.number]).columns
            merged[num_cols] = merged[num_cols].round(2)
            print(f"  Rows: {len(merged)}  Time: {merged[time_col].min()} ~ {merged[time_col].max()}")
            self._write_to_pg(merged, table, time_col, overwrite)


# ══════════════════════════════════════════════════════════════════════════════
# Minestar Importer
# ══════════════════════════════════════════════════════════════════════════════

class MinestarImporter:
    def __init__(self, pg_engine):
        self.engine = pg_engine
        self._ms_engine = None

    def _get_minestar_engine(self):
        if self._ms_engine:
            return self._ms_engine
        params = urllib.parse.quote_plus(
            f"DSN={MINESTAR_DSN};UID={MINESTAR_USER};PWD={MINESTAR_PASSWORD};TrustServerCertificate=yes;"
        )
        self._ms_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        return self._ms_engine

    def _fetch_from_minestar(self, start_date, end_date):
        """Query truck cycle data from Minestar SQL Server."""
        print(f"  Connecting to Minestar (DSN={MINESTAR_DSN})...")
        engine = self._get_minestar_engine()
        sql = f"""
            SELECT *
            FROM {MINESTAR_TABLE}
            WHERE end_timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'
              AND end_timestamp <= '{end_date.strftime('%Y-%m-%d %H:%M:%S')}'
            ORDER BY end_timestamp
        """
        df = pd.read_sql(sql, engine)
        print(f"  Minestar query complete: {len(df)} truck cycle records")
        return df

    def _get_pg_columns(self, table):
        """Return the actual column names of a PG table."""
        with self.engine.connect() as conn:
            result = conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = :t ORDER BY ordinal_position"
            ), {'t': table})
            return [row[0] for row in result]

    def _ensure_schema(self):
        """Ensure new columns exist in truck_cycles (cycle_oid, mprs_create_ts)."""
        new_cols = [
            ('cycle_oid',      'bigint'),
            ('mprs_create_ts', 'timestamp'),
        ]
        with self.engine.connect() as conn:
            for col, dtype in new_cols:
                try:
                    conn.execute(text(
                        f'ALTER TABLE truck_cycles ADD COLUMN IF NOT EXISTS "{col}" {dtype}'
                    ))
                    conn.commit()
                except Exception as e:
                    if 'already exists' not in str(e):
                        print(f"  Warning adding column {col}: {e}")
            # Migrate end_processor → end_processor_group_reporting, then drop redundant columns
            migrations = [
                "UPDATE truck_cycles SET end_processor_group_reporting = end_processor "
                "WHERE end_processor IS NOT NULL AND end_processor_group_reporting IS NULL",
                "ALTER TABLE truck_cycles DROP COLUMN IF EXISTS end_processor",
                "ALTER TABLE truck_cycles DROP COLUMN IF EXISTS ore_waste_block",
            ]
            for sql in migrations:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                except Exception as e:
                    print(f"  Warning (truck_cycles migration): {e}")

    def _transform(self, df):
        """Rename columns, drop unwanted columns, and add the date column."""
        # Drop columns that should not be written to PG
        df = df.drop(columns=[c for c in MINESTAR_DROP_COLS if c in df.columns])

        # Rename columns (Minestar → PG, matching legacy CSV-import column names)
        df = df.rename(columns=MINESTAR_COL_RENAME)

        # Ensure business timestamp columns are timezone-naive datetime (dayfirst=False prevents M/D swap)
        for col in ['start_timestamp', 'end_timestamp']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=False).dt.tz_localize(None)

        # Also convert mprs_create_ts to datetime
        if 'mprs_create_ts' in df.columns:
            df['mprs_create_ts'] = pd.to_datetime(df['mprs_create_ts'], dayfirst=False).dt.tz_localize(None)

        # Add date column (derived from end_timestamp — the date ore reached its destination)
        if 'end_timestamp' in df.columns and 'date' not in df.columns:
            df['date'] = df['end_timestamp'].dt.date

        # Keep only columns that actually exist in PG truck_cycles (auto-filter extras)
        pg_cols = self._get_pg_columns('truck_cycles')
        keep    = [c for c in df.columns if c in pg_cols]
        dropped = [c for c in df.columns if c not in pg_cols]
        pg_only = [c for c in pg_cols if c not in df.columns]

        print(f"\n  ── Column comparison ────────────────────────────")
        print(f"  Minestar columns: {len(df.columns)}  PG columns: {len(pg_cols)}")
        print(f"  Common (will write, {len(keep)}): {keep}")
        if dropped:
            print(f"  Minestar-only (skipped, {len(dropped)}): {dropped}")
        if pg_only:
            print(f"  PG-only (left empty, {len(pg_only)}): {pg_only}")
        print(f"  ────────────────────────────────────────────────\n")

        df = df[keep]

        return df

    def _write_to_pg(self, df, overwrite, start_date, end_date):
        if overwrite:
            with self.engine.connect() as conn:
                conn.execute(text(
                    "DELETE FROM truck_cycles "
                    "WHERE end_timestamp >= :t1 AND end_timestamp <= :t2"
                ), {'t1': start_date, 't2': end_date})
                conn.commit()
            print(f"  [overwrite] Deleted existing rows in truck_cycles for {start_date.date()} ~ {end_date.date()}")
        else:
            with self.engine.connect() as conn:
                existing_df = pd.read_sql(
                    text("SELECT end_timestamp FROM truck_cycles "
                         "WHERE end_timestamp >= :t1 AND end_timestamp <= :t2"),
                    conn, params={'t1': start_date, 't2': end_date}
                )
            existing_ts = set(pd.to_datetime(existing_df['end_timestamp']).dt.tz_localize(None))
            before = len(df)
            df = df[~df['end_timestamp'].isin(existing_ts)].copy()
            print(f"  Skipped existing: {before - len(df)} rows | New rows to write: {len(df)}")
            if df.empty:
                print("  ℹ️  truck_cycles: all data for this period already exists")
                return

        chunk = 500
        for i in range(0, len(df), chunk):
            df.iloc[i:i + chunk].to_sql(
                'truck_cycles', self.engine, if_exists='append', index=False, method='multi'
            )
        print(f"  ✅ truck_cycles write complete ({len(df)} rows)")

    def run(self, start_date, end_date, overwrite=False):
        print(f"\n{'─' * 60}")
        print(f"[Minestar] Starting import  {start_date.date()} ~ {end_date.date()}")
        print(f"           overwrite={overwrite}")
        print(f"{'─' * 60}")
        try:
            self._ensure_schema()
            df = self._fetch_from_minestar(start_date, end_date)
            if df.empty:
                print("  ⚠️  No data returned")
                return
            df = self._transform(df)
            self._write_to_pg(df, overwrite, start_date, end_date)
        except Exception as e:
            print(f"  ❌ Minestar import failed: {e}")
            import traceback
            traceback.print_exc()


# ══════════════════════════════════════════════════════════════════════════════
# Main function
# ══════════════════════════════════════════════════════════════════════════════

def run_import(start_date, end_date, overwrite=False, import_pi=True, import_minestar=True):
    """
    Main import function.

    Args:
        start_date     : datetime, start of import window
        end_date       : datetime, end of import window
        overwrite      : bool, True = delete then write; False = skip existing (default False)
        import_pi      : bool, whether to import PI data (default True)
        import_minestar: bool, whether to import Minestar data (default True)
    """
    print(f"\n{'=' * 60}")
    print(f"Data Import Job")
    print(f"Time range : {start_date}  ~  {end_date}")
    print(f"overwrite  : {overwrite}")
    print(f"PI data    : {'✓' if import_pi else '✗'}")
    print(f"Minestar   : {'✓' if import_minestar else '✗'}")
    print(f"{'=' * 60}")

    pg_engine = create_engine(DB_CONNECTION)

    if import_pi:
        PIImporter(pg_engine).run(start_date, end_date, overwrite=overwrite)

    if import_minestar:
        MinestarImporter(pg_engine).run(start_date, end_date, overwrite=overwrite)

    print(f"\n{'=' * 60}")
    print("✅ All import tasks completed!")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    start_date      = datetime(2025, 4,  1,  0,  0,  0)
    end_date        = datetime(2025, 4, 14, 23, 59, 59)
    overwrite       = True
    import_pi       = True
    import_minestar = True
    run_import(
        start_date      = start_date,
        end_date        = end_date,
        overwrite       = overwrite,
        import_pi       = import_pi,
        import_minestar = import_minestar,
    )
