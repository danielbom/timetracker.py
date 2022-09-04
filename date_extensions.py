from datetime import datetime
from typing import Optional

from constants import CLI_DATE_FORMAT, CLI_HOUR_FORMAT, CLI_PRINT_DATE_FORMAT, DB_DATE_FORMAT


DATE_FORMATS = [
    DB_DATE_FORMAT,
    CLI_PRINT_DATE_FORMAT,
    CLI_DATE_FORMAT,
    CLI_HOUR_FORMAT
]


def try_parse_date(date: str) -> Optional[datetime]:
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(date, fmt)
        except ValueError:
            pass
    return None
