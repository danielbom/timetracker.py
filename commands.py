from datetime import datetime, timedelta
from pathlib import Path

from constants import DATA_DIR, DATA_ENCODING
from core import Row
from date_extensions import try_parse_date
from libraries.micro_sqlite_orm import ALL_ROWS
from repositories import TimetrackerRepository

HOUR_FORMAT = '%H:%M'


class CommandError(Exception):
    pass


class InvalidDateError(CommandError):
    def __init__(self, field) -> None:
        super().__init__(f'Invalid date given for {field}')


def parse_date_or_throw(field, date):
    date = try_parse_date(date)
    if date is None:
        raise InvalidDateError(field)
    return date


def command_list(start='', limit=ALL_ROWS):
    if start == '':
        start = datetime.now() - timedelta(hours=48)
    elif start:
        start = parse_date_or_throw('start', start)
    else:
        start = None

    return TimetrackerRepository.find_many_iter(start, limit)


def command_start(message='', category='', start='', end='') -> Row:
    if not message:
        raise CommandError('No message given')

    if start:
        start = parse_date_or_throw('start', start)

    if start == '':
        start = datetime.now()

    if end:
        end = parse_date_or_throw('end', end)
    else:
        end = None

    new_row = Row(message=message, start=start, category=category, end=end)
    new_row = TimetrackerRepository.create(new_row)
    return new_row


def command_start_in(rowid='', message='', category='') -> Row:
    if not message:
        raise CommandError('No message given')

    if not rowid:
        raise CommandError('No rowid given')

    row = TimetrackerRepository.find_by_id(rowid)
    if not row:
        raise CommandError(f'No row with id {rowid} found')

    if not row.end:
        raise CommandError(f'Row with id {rowid} is still running')

    new_row = Row(message=message, start=row.end, category=category)
    new_row = TimetrackerRepository.create(new_row)
    return new_row


def command_restart(rowid='') -> Row:
    if not rowid:
        raise CommandError('No rowid given')

    row = TimetrackerRepository.find_by_id(rowid)
    if not row:
        raise CommandError(f'No row with id {rowid} found')

    if not row.end:
        raise CommandError(f'Row with id {rowid} is still running')

    new_row = Row(message=row.message, start=row.start, category=row.category)
    new_row = TimetrackerRepository.create(new_row)
    return new_row


def command_end(rowid='') -> Row:
    if not rowid:
        raise CommandError('No rowid given')

    row = TimetrackerRepository.find_by_id(rowid)
    if not row:
        raise CommandError(f'No row with id {rowid} found')

    updated_row = row._replace(end=datetime.now())
    updated_row = TimetrackerRepository.update(updated_row)
    return updated_row


def command_edit(rowid='', message='', category='', start='', end=''):
    if not rowid:
        raise CommandError('No rowid given')

    row = TimetrackerRepository.find_by_id(rowid)
    if not row:
        raise CommandError(f'No row with id {rowid} found')

    if message:
        row = row._replace(message=message)
    if category:
        row = row._replace(category=category)
    if start:
        row = row._replace(start=parse_date_or_throw('start', start))
    if end:
        row = row._replace(end=parse_date_or_throw('end', end))

    updated_row = TimetrackerRepository.update(row)
    return updated_row


def command_drop(rowid=''):
    if not rowid:
        raise CommandError('No rowid given')

    row = TimetrackerRepository.find_by_id(rowid)
    if not row:
        raise CommandError(f'No row with id {rowid} found')

    TimetrackerRepository.delete_by_id(rowid)
    return row


def command_from_csv(input_file=''):
    input_file = Path(input_file)
    if not input_file.is_file():
        raise CommandError(
            f'File {input_file} does not exist or is a directory'
        )

    with input_file.open('r', encoding=DATA_ENCODING) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = Row.from_csv(line)
            TimetrackerRepository.store_or_update(row)


def command_to_csv(output_file=''):
    if not output_file:
        output_file = datetime.now().strftime('%Y-%m-%d-%H-%M-%S.csv')
        output_file = DATA_DIR / output_file
    output_file = Path(output_file)

    rows = TimetrackerRepository.find_many_iter(asc=True)
    with output_file.open('w', encoding=DATA_ENCODING) as f:
        f.writelines(row.to_csv() + '\n' for row in rows)
