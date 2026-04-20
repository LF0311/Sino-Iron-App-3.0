"""
_launcher_processor.py
Interactive launcher for the 4 data-processing scripts (nodes 0 → 3)

Usage: called by Run_data_processor.bat (do not run directly unless conda env is active)
"""
import os
import sys
import importlib.util
from datetime import datetime

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT_DIR     = os.path.dirname(os.path.abspath(__file__))
MINE_TO_MILL = os.path.join(ROOT_DIR, 'mine_to_mill')

os.system('')   # enable ANSI escape codes on Windows 10+
sys.stdout.reconfigure(encoding='utf-8')

# ── Processing scripts: (filename, display name, entry function) ──────────────
SCRIPTS = [
    ('0_Process_Crushers_PG_Delay.py',    'Crushers',             'process_date_range'),
    ('1_Process_Tripper_PG_Delay.py',     'Tripper / CVR',        'process_date_range'),
    ('2_Stockpile_Generation_PG_Delay.py','Stockpile Generation',  'generate_silo_tracking_data'),
    ('3_Mill_Generation_PG_Delay.py',     'Mill Feed Generation',  'generate_mill_feed_data'),
]

# ── Layout constants ──────────────────────────────────────────────────────────
W   = 58
TOP = '╔' + '═' * W + '╗'
SEP = '╠' + '═' * W + '╣'
BOT = '╚' + '═' * W + '╝'

def row(text=''):
    return '║' + text.ljust(W) + '║'

def cls():
    os.system('cls' if os.name == 'nt' else 'clear')

# ── Parameter definitions ─────────────────────────────────────────────────────
PARAMS = [
    ('start_date', 'Start date',  'YYYY-MM-DD', '2025-04-01'),
    ('start_time', 'Start time',  'HH:MM',      '00:00'),
    ('end_date',   'End date',    'YYYY-MM-DD', '2025-04-14'),
    ('end_time',   'End time',    'HH:MM',      '23:59'),
    ('overwrite',  'Overwrite',   'Y/N',        'N'),
]

def fmt(key, val):
    if val is None:
        return '...'
    if key == 'overwrite':
        return 'Yes' if val else 'No'
    return str(val)

# ── Summary box ───────────────────────────────────────────────────────────────
def print_summary(values, enabled_steps, highlight=None):
    cls()
    print(TOP)
    print(row('    Sino Iron — Data Processor  (Nodes 0 → 3)'))
    print(SEP)
    # Script steps
    for i, (_, name, _) in enumerate(SCRIPTS):
        tick = '✓' if enabled_steps[i] else ' '
        print(row(f"    [{tick}] Step {i+1}: {name}"))
    print(SEP)
    # Date/overwrite params
    for i, (key, label, hint, _) in enumerate(PARAMS):
        val  = values.get(key)
        tick = '✓' if val is not None else ' '
        mark = '►' if i == highlight else ' '
        line = f"  {mark}[{tick}] {i+1}. {label:<22} {fmt(key, val)}"
        print(row(line))
    print(BOT)
    print()

# ── Input helpers ─────────────────────────────────────────────────────────────
def ask_date(label, default):
    while True:
        raw = input(f"  {label} [{default}]: ").strip()
        if not raw:
            raw = default
        try:
            datetime.strptime(raw, '%Y-%m-%d')
            return raw
        except ValueError:
            print("  ✗  Invalid format — please use YYYY-MM-DD")

def ask_time(label, default):
    while True:
        raw = input(f"  {label} [{default}]: ").strip()
        if not raw:
            raw = default
        parts = raw.split(':')
        if len(parts) == 2:
            try:
                h, m = int(parts[0]), int(parts[1])
                if 0 <= h <= 23 and 0 <= m <= 59:
                    return f"{h:02d}:{m:02d}"
            except ValueError:
                pass
        print("  ✗  Invalid format — please use HH:MM  (e.g. 08:30)")

def ask_yn(label, default):
    while True:
        raw = input(f"  {label} [{default}]: ").strip()
        if not raw:
            raw = default
        if raw.upper() in ('Y', 'YES'):
            return True
        if raw.upper() in ('N', 'NO'):
            return False
        print("  ✗  Please enter Y or N")

COLLECTORS = {
    'start_date': lambda: ask_date('Start date', '2025-04-01'),
    'start_time': lambda: ask_time('Start time', '00:00'),
    'end_date':   lambda: ask_date('End date',   '2025-04-14'),
    'end_time':   lambda: ask_time('End time',   '23:59'),
    'overwrite':  lambda: ask_yn(  'Overwrite existing data', 'N'),
}

# ── Step selection ────────────────────────────────────────────────────────────
def select_steps(values):
    """Let user toggle which steps to run. Default: all enabled."""
    enabled = [True] * len(SCRIPTS)
    while True:
        print_summary(values, enabled)
        print("  Which steps to run?  (all enabled by default)")
        print("  Type 1-4 to toggle a step  |  Enter to confirm")
        print()
        raw = input("  > ").strip()
        if not raw:
            if not any(enabled):
                print("  ✗  At least one step must be selected. Press Enter...")
                input()
                continue
            return enabled
        for ch in raw:
            try:
                idx = int(ch) - 1
                if 0 <= idx < len(SCRIPTS):
                    enabled[idx] = not enabled[idx]
            except ValueError:
                pass

# ── Collection loop ───────────────────────────────────────────────────────────
def collect_all():
    values   = {}
    enabled  = [True] * len(SCRIPTS)

    for i, (key, label, hint, _) in enumerate(PARAMS):
        print_summary(values, enabled, highlight=i)
        print(f"  ── Field {i+1}/{len(PARAMS)}: {label}  ({hint}) ──")
        values[key] = COLLECTORS[key]()

    enabled = select_steps(values)
    return values, enabled

# ── Confirm / edit loop ───────────────────────────────────────────────────────
def confirm_loop(values, enabled):
    while True:
        print_summary(values, enabled)
        print("  Press Enter to CONFIRM and run")
        print("  Type 1-5 to edit a parameter  |  S to edit steps  |  Q to quit")
        print()
        raw = input("  > ").strip()
        if not raw:
            return True
        if raw.upper() == 'Q':
            print("\n  Cancelled.")
            return False
        if raw.upper() == 'S':
            enabled[:] = select_steps(values)
            continue
        try:
            idx = int(raw) - 1
            if 0 <= idx < len(PARAMS):
                key, label, hint, _ = PARAMS[idx]
                print_summary(values, enabled, highlight=idx)
                print(f"  ── Re-enter Field {idx+1}: {label}  ({hint}) ──")
                values[key] = COLLECTORS[key]()
            else:
                input(f"  ✗  Please enter a number between 1 and {len(PARAMS)}. Press Enter...")
        except ValueError:
            input("  ✗  Invalid input. Press Enter to continue...")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    values, enabled = collect_all()
    if not confirm_loop(values, enabled):
        sys.exit(0)

    start_dt = datetime.strptime(f"{values['start_date']} {values['start_time']}:00", '%Y-%m-%d %H:%M:%S')
    end_dt   = datetime.strptime(f"{values['end_date']}   {values['end_time']}:59",   '%Y-%m-%d %H:%M:%S')
    overwrite = values['overwrite']

    cls()
    print(TOP)
    print(row('    Running Data Processor...'))
    print(SEP)
    print(row(f"    Start    : {start_dt}"))
    print(row(f"    End      : {end_dt}"))
    print(row(f"    Overwrite: {'Yes' if overwrite else 'No'}"))
    print(SEP)
    selected = [(i, s) for i, s in enumerate(SCRIPTS) if enabled[i]]
    for i, (_, name, _) in selected:
        print(row(f"    Step {i+1}: {name}"))
    print(BOT)
    print()

    # chdir to mine_to_mill so that any "./data/..." paths in scripts resolve correctly
    os.chdir(MINE_TO_MILL)
    sys.path.insert(0, MINE_TO_MILL)
    total   = len(selected)
    results = []

    for step_num, (i, (filename, name, func_name)) in enumerate(selected, 1):
        print(f"\n  {'─'*56}")
        print(f"  [{step_num}/{total}] {name}")
        print(f"  {'─'*56}")
        path = os.path.join(MINE_TO_MILL, filename)
        try:
            spec = importlib.util.spec_from_file_location(f'script_{i}', path)
            mod  = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            func = getattr(mod, func_name)
            func(start_dt, end_dt, overwrite)
            results.append((name, True, None))
            print(f"\n  ✓  Step {step_num} — {name} — completed")
        except Exception as e:
            results.append((name, False, str(e)))
            print(f"\n  ✗  Step {step_num} — {name} — FAILED: {e}")
            raw = input("\n  Continue with remaining steps? [Y/N]: ").strip()
            if raw.upper() not in ('Y', 'YES'):
                break

    # Final summary
    print()
    print(TOP)
    print(row('    Processing Summary'))
    print(SEP)
    for name, ok, err in results:
        tick = '✓' if ok else '✗'
        msg  = 'OK' if ok else f'FAILED: {err[:30]}'
        print(row(f"    [{tick}] {name:<35} {msg}"))
    print(BOT)
    input("\n  Press Enter to exit...")


if __name__ == '__main__':
    main()
