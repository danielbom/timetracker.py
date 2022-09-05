from datetime import datetime, timedelta
from typing import NamedTuple, Optional
from constants import DATA_ENCODING, DB_DATE_FORMAT, DB_PATH, CSV_DATE_FORMAT, CSV_ROW_SEP
from database import db_up

from libraries.micro_sqlite_orm import MicroSqliteORM


with MicroSqliteORM(DB_PATH) as orm:
    db_up(orm.cursor)


def normalize_date(date: datetime) -> datetime:
    return date.replace(second=0, microsecond=0)


def parse_date_csv(date: str) -> datetime:
    # https://stackoverflow.com/questions/127803/how-do-i-parse-an-iso-8601-formatted-date
    return datetime.strptime(date, CSV_DATE_FORMAT)


def parse_date_db(date: str) -> datetime:
    return datetime.strptime(date.replace(" ", "T"), DB_DATE_FORMAT)


def clean_message(message: str) -> str:
    return message.strip('"').replace('"', "'").strip()


def parse_message_db(message: str) -> str:
    try:
        return message.encode("latin-1").decode(DATA_ENCODING)
    except UnicodeDecodeError:
        return message


class Row(NamedTuple):
    start: datetime
    message: str
    end: Optional[datetime] = None
    category: str = ""
    rowid: int = 0

    @staticmethod
    def from_csv(line: str) -> "Row":
        parts = line.strip().split(CSV_ROW_SEP)
        return Row(
            start=parse_date_csv(parts[0]),
            message=parts[1],
            end=parse_date_csv(parts[2]) if parts[2] else None,
            category=parts[3]
        ).normalize()

    @staticmethod
    def from_db(row: list):
        # https://stackoverflow.com/questions/492483/setting-the-correct-encoding-when-piping-stdout-in-python
        return Row(
            start=parse_date_db(row[0]),
            message=parse_message_db(row[1]),
            end=parse_date_db(row[2]) if row[2] else None,
            category=row[3].strip(),
            rowid=row[4]
        ).normalize()

    def to_dict_db(self) -> dict:
        return {
            "start": self.start.strftime(DB_DATE_FORMAT),
            "message": self.message,
            "end": self.end.strftime(DB_DATE_FORMAT) if self.end else "",
            "category": self.category.strip() or "",
        }

    def to_dict_csv(self) -> dict:
        return {
            "start": normalize_date(self.start).strftime(CSV_DATE_FORMAT),
            "message": f'"{clean_message(self.message)}"',
            "end": normalize_date(self.end).strftime(CSV_DATE_FORMAT) if self.end else "",
            "category": self.category.strip() or "",
        }

    def to_csv(self) -> str:
        return CSV_ROW_SEP.join(self.to_dict_csv().values())

    def normalize(self) -> "Row":
        return Row(
            start=normalize_date(self.start),
            message=clean_message(self.message),
            end=normalize_date(self.end) if self.end else None,
            category=self.category,
            rowid=self.rowid
        )

    @property
    def duration(self) -> timedelta:
        return self.end - self.start if self.end else timedelta(0)
