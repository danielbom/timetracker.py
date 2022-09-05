from datetime import datetime
from pathlib import Path
from constants import DATA_DIR, DATA_ENCODING
from core import Row
from date_extensions import try_parse_date
from libraries.micro_sqlite_orm import ALL_ROWS
from repositories import TimetrackerRepository

HOUR_FORMAT = "%H:%M"


class CommandError(Exception):
    pass


def command_list(date="", limit=ALL_ROWS):
    if date == "today":
        date = datetime.now()
    elif date:
        date = try_parse_date(date)
    else:
        date = None

    return TimetrackerRepository.find_many_iter(date, limit)


def command_start(message="", category="", start="", end="") -> Row:
    if not message:
        raise CommandError("No message given")

    if start:
        start = try_parse_date(start)

    if start is None or start == "":
        start = datetime.now()

    if end:
        end = try_parse_date(end)
        if end is None:
            raise CommandError("Invalid end date given")
    else:
        end = None

    new_row = Row(message=message, start=start, category=category, end=end)
    new_row = TimetrackerRepository.create(new_row)
    return new_row


def command_start_in(rowid="", message="", category="") -> Row:
    if not message:
        raise CommandError("No message given")

    if not rowid:
        raise CommandError("No rowid given")

    row = TimetrackerRepository.find_by_id(rowid)
    if not row:
        raise CommandError(f"No row with id {rowid} found")

    if not row.end:
        raise CommandError(f"Row with id {rowid} is still running")

    new_row = Row(message=message, start=row.end, category=category)
    new_row = TimetrackerRepository.create(new_row)
    return new_row


def command_restart(rowid="") -> Row:
    if not rowid:
        raise CommandError("No rowid given")

    row = TimetrackerRepository.find_by_id(rowid)
    if not row:
        raise CommandError(f"No row with id {rowid} found")

    if not row.end:
        raise CommandError(f"Row with id {rowid} is still running")

    new_row = Row(message=row.message, start=row.start, category=row.category)
    new_row = TimetrackerRepository.create(new_row)
    return new_row


def command_end(rowid="") -> Row:
    if not rowid:
        raise CommandError("No rowid given")

    row = TimetrackerRepository.find_by_id(rowid)
    if not row:
        raise CommandError(f"No row with id {rowid} found")

    updated_row = row._replace(end=datetime.now())
    updated_row = TimetrackerRepository.update(updated_row)
    return updated_row


def command_from_csv(input_file=""):
    input_file = Path(input_file)
    if not input_file.is_file():
        raise CommandError(
            f"File {input_file} does not exist or is a directory")

    with input_file.open("r", encoding=DATA_ENCODING) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = Row.from_csv(line)
            TimetrackerRepository.store_or_update(row)


def command_to_csv(output_file=""):
    if not output_file:
        output_file = datetime.now().strftime("%Y-%m-%d-%H-%M-%S.csv")
        output_file = DATA_DIR / output_file
    output_file = Path(output_file)

    rows = TimetrackerRepository.find_many_iter(asc=True)
    with output_file.open("w", encoding=DATA_ENCODING) as f:
        f.writelines(row.to_csv() + "\n" for row in rows)
 