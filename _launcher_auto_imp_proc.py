"""
_launcher_auto_imp_proc.py
Auto-scheduler: runs data import + all 4 processing nodes on a fixed interval.

Each cycle covers: (now - interval - buffer)  →  now
Uses overwrite=False (incremental — skips already-existing records).

State file : ./data/auto_scheduler_state.json  (last-run time + cycle count)
Log file   : ./data/auto_scheduler.log         (one line per cycle, appended forever)

On restart : if the scheduler is restarted within the interval window, it reads
             the state file and resumes the countdown from the remaining time
             rather than running again immediately.

Usage: called by Run_auto.bat (do not run directly unless conda env is active)
Stop : Ctrl+C
"""

import os
import sys
import importlib.util
import time
import json
from datetime import datetime, timedelta

DB_CONNECTION = 'postgresql://postgres:postgres@localhost:5432/postgres'

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT_DIR              = os.path.dirname(os.path.abspath(__file__))
MINE_TO_MILL          = os.path.join(ROOT_DIR, 'mine_to_mill')
IMPORTER_PATH         = os.path.join(MINE_TO_MILL, 'Import_PI_Minestar_to_PG_v1-1.py')
DATA_DIR              = os.path.join(ROOT_DIR, 'data')
STATE_FILE            = os.path.join(DATA_DIR, 'auto_scheduler_state.json')
LOG_FILE              = os.path.join(DATA_DIR, 'auto_scheduler.log')
SCHEDULER_CONFIG_PATH = os.path.join(ROOT_DIR, 'resources', 'scheduler.config')

os.system('')   # enable ANSI escape codes on Windows 10+
sys.stdout.reconfigure(encoding='utf-8')

# ── Processing scripts ────────────────────────────────────────────────────────
SCRIPTS = [
    ('0_Process_Crushers_PG_Delay.py',    'Crushers',            'process_date_range'),
    ('1_Process_Tripper_PG_Delay.py',     'Tripper / CVR',       'process_date_range'),
    ('2_Stockpile_Generation_PG_Delay.py','Stockpile Generation', 'generate_silo_tracking_data'),
    ('3_Mill_Generation_PG_Delay.py',     'Mill Feed',            'generate_mill_feed_data'),
]

# ── Layout ────────────────────────────────────────────────────────────────────
W   = 58
TOP = '╔' + '═' * W + '╗'
SEP = '╠' + '═' * W + '╣'
BOT = '╚' + '═' * W + '╝'

def row(text=''):
    return '║' + text.ljust(W) + '║'

def cls():
    os.system('cls' if os.name == 'nt' else 'clear')

# ── Scheduler config ──────────────────────────────────────────────────────────
def load_scheduler_config():
    defaults = {
        'interval_min':    30,
        'buffer_min':       5,
        'import_pi':      True,
        'import_minestar': True,
        'unattended':     False,
    }
    try:
        with open(SCHEDULER_CONFIG_PATH, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        defaults.update(cfg)
        print(f"✅ Scheduler config loaded: interval={defaults['interval_min']}min  "
              f"buffer={defaults['buffer_min']}min  "
              f"unattended={defaults['unattended']}")
    except Exception as e:
        print(f"⚠️  Failed to read scheduler.config, using defaults: {e}")
    return defaults

# ── DB last-time fallback (used when state file is absent) ───────────────────
def get_db_last_time():
    """Query MAX(time) from mill_feed (final processing output) for catch-up reference."""
    try:
        from sqlalchemy import create_engine, text as sql_text
        engine = create_engine(DB_CONNECTION, pool_pre_ping=True)
        with engine.connect() as conn:
            result = conn.execute(sql_text('SELECT MAX(time) FROM mill_feed'))
            last = result.scalar()
        engine.dispose()
        if last:
            return datetime.strptime(str(last)[:19], '%Y-%m-%d %H:%M:%S')
    except Exception:
        pass
    return None

# ── State file ────────────────────────────────────────────────────────────────
def load_state():
    """Return state dict, or empty dict if no state file exists."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_state(last_run: datetime, cycle_count: int, interval_min: int):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            'last_run':     last_run.strftime('%Y-%m-%d %H:%M:%S'),
            'cycle_count':  cycle_count,
            'interval_min': interval_min,
        }, f, indent=2)

# ── Log file ──────────────────────────────────────────────────────────────────
def write_log(cycle_num: int, start_dt: datetime, end_dt: datetime, results: list):
    os.makedirs(DATA_DIR, exist_ok=True)
    status_parts = [f"{name}:{'OK' if ok else 'FAIL'}" for name, ok, _ in results]
    line = (
        f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
        f"Cycle #{cycle_num:04d} | "
        f"Window: {start_dt.strftime('%Y-%m-%d %H:%M')} ~ {end_dt.strftime('%Y-%m-%d %H:%M')} | "
        f"{' | '.join(status_parts)}\n"
    )
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line)
    print(f"  📄 Logged → {LOG_FILE}")

# ── Input helpers ─────────────────────────────────────────────────────────────
def ask_interval(last_interval=None):
    default = str(last_interval) if last_interval else '30'
    while True:
        raw = input(f"  Interval (minutes) [{default}]: ").strip()
        if not raw:
            return int(default)
        try:
            val = int(raw)
            if val >= 1:
                return val
        except ValueError:
            pass
        print("  ✗  Please enter a whole number of minutes (e.g. 30, 60, 120)")

def ask_buffer():
    while True:
        raw = input("  Overlap buffer (minutes) [5]: ").strip()
        if not raw:
            return 5
        try:
            val = int(raw)
            if val >= 0:
                return val
        except ValueError:
            pass
        print("  ✗  Please enter 0 or a positive integer")

def ask_yn(label, default='Y'):
    while True:
        raw = input(f"  {label} [{default}]: ").strip()
        if not raw:
            raw = default
        if raw.upper() in ('Y', 'YES'):
            return True
        if raw.upper() in ('N', 'NO'):
            return False
        print("  ✗  Please enter Y or N")

# ── Confirm screen ────────────────────────────────────────────────────────────
def print_config(interval_min, buffer_min, import_pi, import_minestar,
                 cycle_num=None, last_run=None, unattended=False):
    cls()
    print(TOP)
    title = '    Sino Iron — Auto Scheduler'
    if cycle_num:
        title += f'  (Cycle #{cycle_num})'
    print(row(title))
    print(SEP)
    print(row(f'    Mode      : {"Unattended (auto-start)" if unattended else "Interactive"}'))
    print(row(f'    Interval  : every {interval_min} min'))
    print(row(f'    Window    : last {interval_min + buffer_min} min  (buffer +{buffer_min} min)'))
    print(row(f'    Import PI : {"Yes" if import_pi else "No"}'))
    print(row(f'    Minestar  : {"Yes" if import_minestar else "No"}'))
    print(row(f'    Overwrite : No  (incremental append)'))
    if last_run:
        print(row(f'    Last run  : {last_run}'))
    print(row(f'    State     : {STATE_FILE}'))
    print(row(f'    Log       : {LOG_FILE}'))
    print(SEP)
    for i, (_, name, _) in enumerate(SCRIPTS):
        print(row(f'    [✓] Step {i+1}: {name}'))
    print(BOT)
    print()

# ── Load module helper ────────────────────────────────────────────────────────
def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

# ── Single cycle ──────────────────────────────────────────────────────────────
def run_cycle(interval_min, buffer_min, import_pi, import_minestar, cycle_num, override_start=None):
    end_dt   = datetime.now().replace(second=0, microsecond=0)
    start_dt = override_start if override_start is not None else (end_dt - timedelta(minutes=interval_min + buffer_min))

    print(f"\n  ─── Cycle #{cycle_num}  started {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ───")
    print(f"  Window : {start_dt}  →  {end_dt}\n")

    results = []

    # ── Import ────────────────────────────────────────────────────────────────
    if import_pi or import_minestar:
        print(f"  {'─'*54}")
        print(f"  [Import] PI={import_pi}  Minestar={import_minestar}")
        print(f"  {'─'*54}")
        try:
            os.chdir(MINE_TO_MILL)
            sys.path.insert(0, MINE_TO_MILL)
            mod = load_module(IMPORTER_PATH, 'importer')
            mod.run_import(
                start_date      = start_dt,
                end_date        = end_dt,
                overwrite       = False,
                import_pi       = import_pi,
                import_minestar = import_minestar,
            )
            results.append(('Import', True, None))
            print(f"\n  ✓  Import — completed")
        except Exception as e:
            results.append(('Import', False, str(e)))
            print(f"\n  ✗  Import — FAILED: {e}")

    # ── Processing nodes ──────────────────────────────────────────────────────
    for i, (filename, name, func_name) in enumerate(SCRIPTS, 1):
        print(f"\n  {'─'*54}")
        print(f"  [{i}/{len(SCRIPTS)}] {name}")
        print(f"  {'─'*54}")
        path = os.path.join(MINE_TO_MILL, filename)
        try:
            os.chdir(MINE_TO_MILL)
            mod  = load_module(path, f'script_{i}')
            func = getattr(mod, func_name)
            func(start_dt, end_dt, False)
            results.append((name, True, None))
            print(f"\n  ✓  {name} — completed")
        except Exception as e:
            results.append((name, False, str(e)))
            print(f"\n  ✗  {name} — FAILED: {e}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print(TOP)
    print(row(f'    Cycle #{cycle_num} Summary'))
    print(SEP)
    for name, ok, err in results:
        tick = '✓' if ok else '✗'
        msg  = 'OK' if ok else f'FAILED: {err[:28]}'
        print(row(f'    [{tick}] {name:<32} {msg}'))
    print(BOT)

    # ── Persist state + log ───────────────────────────────────────────────────
    now = datetime.now().replace(second=0, microsecond=0)
    save_state(now, cycle_num, interval_min)
    write_log(cycle_num, start_dt, end_dt, results)

    return results

# ── Countdown ─────────────────────────────────────────────────────────────────
def countdown(seconds, interval_min, cycle_num):
    print()
    try:
        for remaining in range(seconds, 0, -1):
            mm, ss = divmod(remaining, 60)
            hh, mm = divmod(mm, 60)
            print(
                f"\r  Cycle #{cycle_num + 1} in: {hh:02d}:{mm:02d}:{ss:02d}"
                f"  (interval={interval_min}min | Ctrl+C to stop)  ",
                end='', flush=True
            )
            time.sleep(1)
        print()
    except KeyboardInterrupt:
        print("\n\n  Stopping scheduler...")
        raise

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # ── Load scheduler config ─────────────────────────────────────────────────
    cfg             = load_scheduler_config()
    unattended      = cfg['unattended']
    interval_min    = cfg['interval_min']
    buffer_min      = cfg['buffer_min']
    import_pi       = cfg['import_pi']
    import_minestar = cfg['import_minestar']

    # ── Check state file for previous run ────────────────────────────────────
    state = load_state()
    last_run_dt   = None
    resume_cycle  = 1
    resume_wait_s = 0

    if state:
        try:
            last_run_dt   = datetime.strptime(state['last_run'], '%Y-%m-%d %H:%M:%S')
            resume_cycle  = state.get('cycle_count', 0) + 1
            last_interval = state.get('interval_min', interval_min)
            elapsed_s     = (datetime.now() - last_run_dt).total_seconds()
            remaining_s   = last_interval * 60 - elapsed_s
            if remaining_s > 0:
                resume_wait_s = int(remaining_s)
        except Exception:
            pass

    # ── Interactive mode: override config with user input ─────────────────────
    if not unattended:
        cls()
        print(TOP)
        print(row('    Sino Iron — Auto Scheduler Setup'))
        if last_run_dt:
            print(SEP)
            print(row(f'    Previous state found:'))
            print(row(f'    Last run : {last_run_dt.strftime("%Y-%m-%d %H:%M")}'))
            print(row(f'    Cycles   : {resume_cycle - 1}'))
            if resume_wait_s > 0:
                mm, ss = divmod(resume_wait_s, 60)
                hh, mm = divmod(mm, 60)
                print(row(f'    Resume in: {hh:02d}:{mm:02d}:{ss:02d}  (skipping immediate run)'))
        print(BOT)
        print()
        print("  Configure the auto-run schedule:")
        print()
        interval_min    = ask_interval(cfg['interval_min'])
        buffer_min      = ask_buffer()
        import_pi       = ask_yn('Import PI data each cycle',       'Y')
        import_minestar = ask_yn('Import Minestar data each cycle', 'Y')

    last_run_str = last_run_dt.strftime('%Y-%m-%d %H:%M') if last_run_dt else 'None'
    print_config(interval_min, buffer_min, import_pi, import_minestar,
                 last_run=last_run_str, unattended=unattended)

    # ── Confirm / auto-start ──────────────────────────────────────────────────
    force_run = False
    if unattended:
        if resume_wait_s > 0:
            mm, ss = divmod(resume_wait_s, 60)
            hh, mm = divmod(mm, 60)
            print(f"  ✅ Unattended mode — resuming countdown: {hh:02d}:{mm:02d}:{ss:02d} remaining")
        else:
            print(f"  ✅ Unattended mode — starting first cycle now")
    else:
        if resume_wait_s > 0:
            mm2, ss2 = divmod(resume_wait_s, 60)
            hh2, mm2 = divmod(mm2, 60)
            print(f"  ⚡ Last run was {int((interval_min*60 - resume_wait_s)/60)} min ago.")
            print(f"     Will resume countdown ({hh2:02d}:{mm2:02d}:{ss2:02d} remaining).")
            print(f"     Press Enter to CONFIRM  |  R to run immediately  |  Q to quit")
        else:
            print("  Press Enter to START  |  Q to quit")

        raw = input("  > ").strip().upper()
        if raw == 'Q':
            print("\n  Cancelled.")
            sys.exit(0)
        if raw == 'R':
            force_run = True

    cycle_num = resume_cycle

    # ── Catch-up reference time: DB is ground truth, state is hint ──────────────
    # Always query DB regardless of state file. If both exist and agree within
    # 30 min, trust state (faster). If they diverge > 30 min, trust DB (state
    # may reflect a partial/aborted run).
    db_last_time = get_db_last_time()

    catchup_ref = None
    if last_run_dt and db_last_time:
        diff_min = abs((last_run_dt - db_last_time).total_seconds() / 60)
        if diff_min <= 30:
            catchup_ref = last_run_dt
            print(f"  ℹ️  State/DB in sync (diff={diff_min:.0f} min) — reference: {catchup_ref.strftime('%Y-%m-%d %H:%M')}")
        else:
            catchup_ref = db_last_time
            print(f"  ⚠️  State/DB discrepancy ({diff_min:.0f} min) — using DB time: {catchup_ref.strftime('%Y-%m-%d %H:%M')}")
    elif last_run_dt:
        catchup_ref = last_run_dt
    elif db_last_time:
        catchup_ref = db_last_time
        print(f"  ℹ️  No state file — DB last data time: {catchup_ref.strftime('%Y-%m-%d %H:%M')}")

    catchup_start = None
    if catchup_ref and resume_wait_s <= 0:
        gap_min = (datetime.now() - catchup_ref).total_seconds() / 60
        if gap_min > interval_min + buffer_min:
            catchup_start = catchup_ref - timedelta(minutes=buffer_min)
            print(f"  ⚡ Gap detected ({int(gap_min)} min) — catch-up cycle from {catchup_start.strftime('%Y-%m-%d %H:%M')}")

    try:
        if resume_wait_s > 0 and not force_run:
            print_config(interval_min, buffer_min, import_pi, import_minestar,
                         cycle_num=cycle_num, last_run=last_run_str, unattended=unattended)
            countdown(resume_wait_s, interval_min, cycle_num - 1)

        while True:
            print_config(interval_min, buffer_min, import_pi, import_minestar,
                         cycle_num=cycle_num, unattended=unattended)
            if catchup_start is not None:
                run_cycle(interval_min, buffer_min, import_pi, import_minestar, cycle_num,
                          override_start=catchup_start)
                catchup_start = None
            else:
                run_cycle(interval_min, buffer_min, import_pi, import_minestar, cycle_num)
            countdown(interval_min * 60, interval_min, cycle_num)
            cycle_num += 1

    except KeyboardInterrupt:
        print()
        print(TOP)
        print(row(f'    Scheduler stopped after {cycle_num - resume_cycle} new cycle(s).'))
        print(row(f'    Total cycles logged: {cycle_num - 1}'))
        print(row(f'    Log: {os.path.basename(LOG_FILE)}'))
        print(BOT)
        print()


if __name__ == '__main__':
    main()
