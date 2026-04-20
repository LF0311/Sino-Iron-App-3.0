"""
_launcher_importer.py
Interactive launcher for Import_PI_Minestar_to_PG_v1-1.py

Usage: called by Run_data_importer.bat (do not run directly unless conda env is active)
"""
import os
import sys
import importlib.util
from datetime import datetime

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT_DIR      = os.path.dirname(os.path.abspath(__file__))
MINE_TO_MILL  = os.path.join(ROOT_DIR, 'mine_to_mill')
IMPORTER_PATH = os.path.join(MINE_TO_MILL, 'Import_PI_Minestar_to_PG_v1-1.py')

os.system('')   # enable ANSI escape codes on Windows 10+
sys.stdout.reconfigure(encoding='utf-8')

# ── Layout constants ──────────────────────────────────────────────────────────
W   = 58   # inner width (between ║ characters)
TOP = '╔' + '═' * W + '╗'
SEP = '╠' + '═' * W + '╣'
BOT = '╚' + '═' * W + '╝'

def row(text=''):
    return '║' + text.ljust(W) + '║'

def cls():
    os.system('cls' if os.name == 'nt' else 'clear')

# ── Parameter definitions ─────────────────────────────────────────────────────
#   (key, display_label, hint, default_value)
PARAMS = [
    ('start_date',      'Start date',       'YYYY-MM-DD',  '2025-04-01'),
    ('start_time',      'Start time',       'HH:MM',       '00:00'),
    ('end_date',        'End date',         'YYYY-MM-DD',  '2025-04-14'),
    ('end_time',        'End time',         'HH:MM',       '23:59'),
    ('overwrite',       'Overwrite data',   'Y/N',         'N'),
    ('import_pi',       'Import PI data',   'Y/N',         'Y'),
    ('import_minestar', 'Import Minestar',  'Y/N',         'Y'),
]

def fmt(key, val):
    """Format a value for display in the summary box."""
    if val is None:
        return '...'
    if key in ('overwrite', 'import_pi', 'import_minestar'):
        return 'Yes' if val else 'No'
    return str(val)

# ── Summary box ───────────────────────────────────────────────────────────────
def print_summary(values, highlight=None):
    cls()
    print(TOP)
    print(row('    Sino Iron — Data Importer  (PI + Minestar)'))
    print(SEP)
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

# Map each key to its collector function
COLLECTORS = {
    'start_date':      lambda: ask_date('Start date', '2025-04-01'),
    'start_time':      lambda: ask_time('Start time', '00:00'),
    'end_date':        lambda: ask_date('End date',   '2025-04-14'),
    'end_time':        lambda: ask_time('End time',   '23:59'),
    'overwrite':       lambda: ask_yn(  'Overwrite existing data', 'N'),
    'import_pi':       lambda: ask_yn(  'Import PI data',          'Y'),
    'import_minestar': lambda: ask_yn(  'Import Minestar data',    'Y'),
}

# ── Collection loop ───────────────────────────────────────────────────────────
def collect_all():
    values = {}
    for i, (key, label, hint, _) in enumerate(PARAMS):
        print_summary(values, highlight=i)
        print(f"  ── Field {i+1}/{len(PARAMS)}: {label}  ({hint}) ──")
        values[key] = COLLECTORS[key]()
    return values

# ── Confirm / edit loop ───────────────────────────────────────────────────────
def confirm_loop(values):
    while True:
        print_summary(values)
        print("  Press Enter to CONFIRM and run")
        print("  Type 1-7 to edit a field   |   Q to quit")
        print()
        raw = input("  > ").strip()
        if not raw:
            return True
        if raw.upper() == 'Q':
            print("\n  Cancelled.")
            return False
        try:
            idx = int(raw) - 1
            if 0 <= idx < len(PARAMS):
                key, label, hint, _ = PARAMS[idx]
                print_summary(values, highlight=idx)
                print(f"  ── Re-enter Field {idx+1}: {label}  ({hint}) ──")
                values[key] = COLLECTORS[key]()
            else:
                input(f"  ✗  Please enter a number between 1 and {len(PARAMS)}. Press Enter...")
        except ValueError:
            input("  ✗  Invalid input. Press Enter to continue...")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    values = collect_all()
    if not confirm_loop(values):
        sys.exit(0)

    # Build datetime objects
    # start_time → HH:MM:00,  end_time → HH:MM:59
    start_dt = datetime.strptime(f"{values['start_date']} {values['start_time']}:00", '%Y-%m-%d %H:%M:%S')
    end_dt   = datetime.strptime(f"{values['end_date']}   {values['end_time']}:59",   '%Y-%m-%d %H:%M:%S')

    cls()
    print(TOP)
    print(row('    Running Data Import...'))
    print(SEP)
    print(row(f"    Start    : {start_dt}"))
    print(row(f"    End      : {end_dt}"))
    print(row(f"    Overwrite: {'Yes' if values['overwrite'] else 'No'}"))
    print(row(f"    PI       : {'Yes' if values['import_pi'] else 'No'}"))
    print(row(f"    Minestar : {'Yes' if values['import_minestar'] else 'No'}"))
    print(BOT)
    print()

    # Dynamically load the importer module (filename contains hyphens)
    # chdir to mine_to_mill so that "./data/..." paths inside the script resolve correctly
    os.chdir(MINE_TO_MILL)
    sys.path.insert(0, MINE_TO_MILL)
    spec = importlib.util.spec_from_file_location('importer', IMPORTER_PATH)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    mod.run_import(
        start_date      = start_dt,
        end_date        = end_dt,
        overwrite       = values['overwrite'],
        import_pi       = values['import_pi'],
        import_minestar = values['import_minestar'],
    )

    print()
    print(TOP)
    print(row('    ✓  Import Completed Successfully!'))
    print(BOT)
    input("\n  Press Enter to exit...")


if __name__ == '__main__':
    main()
