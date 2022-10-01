from pathlib import Path

ROOT_DIR = Path(__file__).parent

DATA_DIR = ROOT_DIR / 'data'
DATA_ENCODING = 'utf-8'

DB_PATH = DATA_DIR / 'data.db'
DB_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

CSV_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
CSV_ROW_SEP = ';'

CLI_PRINT_DATE_FORMAT = '%Y-%m-%d %H:%M'
CLI_DATE_FORMAT = '%Y/%m/%d'
CLI_HOUR_FORMAT = '%H:%M'
