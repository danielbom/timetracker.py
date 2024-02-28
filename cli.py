import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import groupby
from pathlib import Path
import sqlite3
from typing import Optional
from constants import CLI_DATE_FORMAT, CLI_HOUR_FORMAT, CLI_PRINT_DATE_FORMAT, DB_DATE_FORMAT, DB_PATH
import json

from date_extensions import DATE_FORMATS, parse_date_db, try_parse_date

UNSET = object()


def format_duration(duration: timedelta):
    hours, remainder = divmod(duration.total_seconds(), 3600)
    minutes, _seconds = divmod(remainder, 60)
    return f'{int(hours):02d}:{int(minutes):02d}'


@dataclass
class Timetracker:
    rowid: int
    message: str
    start: datetime
    end: Optional[datetime]
    category: Optional[str]

    @classmethod
    def from_row(cls, row: tuple):
        return cls(row[0], row[1], parse_date_db(row[2]), row[3] and parse_date_db(row[3]), row[4])

    def show(self, now: Optional[datetime] = None, rowid_len: int = 0):
        # https://stackoverflow.com/questions/31018497/how-to-format-duration-in-python-timedelta
        if now is None:
            now = datetime.now()
        duration_delta = (self.end or now) - self.start
        duration = format_duration(duration_delta)
        if duration_delta > timedelta(days=1):
            start = self.start.strftime(CLI_PRINT_DATE_FORMAT)
            end = ' ' * 15
            if self.end:
                end = self.end.strftime(CLI_PRINT_DATE_FORMAT)
            duration = format_duration((self.end or now) - self.start)
            rowid = str(self.rowid).rjust(rowid_len)
            print(f'{rowid}: {start} .. {end} | {duration} {self.message}')
        else:
            start_day = self.start.strftime(CLI_DATE_FORMAT)
            start = self.start.strftime(CLI_HOUR_FORMAT)
            end = self.end.strftime(CLI_HOUR_FORMAT) if self.end else '--:--'
            rowid = str(self.rowid).rjust(rowid_len)
            print(
                f'{rowid}: {start_day} | {start} .. {end} | {duration} | {self.message}')


def batched(values, batch_size):
    for batch_ix in range(0, len(values), batch_size):
        yield values[batch_ix:batch_ix + batch_size]


class CommandError(Exception):
    pass


class InvalidDateError(CommandError):
    def __init__(self, field) -> None:
        super().__init__(
            f'Invalid date given for {field}\nValid formats: {", ".join(DATE_FORMATS)}')


def get_cursor(connection: sqlite3.Connection) -> sqlite3.Cursor:
    cursor = connection.cursor()
    cursor.execute('PRAGMA foreign_keys=ON')
    cursor.execute('PRAGMA encoding=utf8')
    return cursor


def parse_date_or_throw(field, date):
    date = try_parse_date(date)
    if date is None:
        raise InvalidDateError(field)
    return date


class CommandSetup(argparse.Namespace):
    database_path: str = DB_PATH


def command_setup(args: CommandSetup):
    "Setup the database"
    connection = sqlite3.connect(args.database_path)
    cursor = get_cursor(connection)
    cursor.execute('PRAGMA foreign_keys=ON')
    cursor.execute('PRAGMA encoding=utf8')
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS timetrack ("
        "  start DATETIME NOT NULL,"
        "  message TEXT NOT NULL,"
        "  end DATETIME,"
        "  category TEXT"
        ")"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS timetrack_start ON timetrack (start DESC)"
    )
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS timetrack_start_message ON timetrack (start, message)"
    )
    cursor.execute('pragma encoding')


class CommandStart(argparse.Namespace):
    message: str
    category: Optional[str]
    start: Optional[str]
    end: Optional[str]


def command_start(args: CommandStart):
    "Start a new time tracking entry"
    start = datetime.now().strftime(DB_DATE_FORMAT)
    end = None
    if args.start is not None:
        start = parse_date_or_throw('start', args.start)
        start = start.strftime(DB_DATE_FORMAT)

    if end is not None:
        end = parse_date_or_throw('end', args.end)
        end = end.strftime(DB_DATE_FORMAT)

    connection = sqlite3.connect(DB_PATH)
    cursor = get_cursor(connection)
    cursor.execute(
        'INSERT INTO timetrack (message, start, end, category) '
        'VALUES (?, ?, ?, ?) '
        'RETURNING rowid, message, start, end, category',
        (args.message, start, end, args.category)
    )
    row = cursor.fetchone()
    entity = Timetracker.from_row(row)
    connection.commit()
    connection.close()
    entity.show()


class CommandStartIn(argparse.Namespace):
    id: int
    message: str
    category: Optional[str]


def command_start_in(args):
    "Start a new time tracking entry in the end of other entry"

    connection = sqlite3.connect(DB_PATH)
    cursor = get_cursor(connection)
    cursor.execute(
        'SELECT end FROM timetrack WHERE rowid = ?',
        (args.id,)
    )
    row = cursor.fetchone()
    if row is None:
        raise CommandError(f'No row with id {args.id} found')

    if row[0] is None:
        raise CommandError(f'Row with id {args.id} is still running')

    cursor.execute(
        'INSERT INTO timetrack (message, start, end, category) '
        'VALUES (?, ?, ?, ?) '
        'RETURNING rowid, message, start, end, category',
        (args.message, row[0], None, args.category)
    )
    row = cursor.fetchone()
    entity = Timetracker.from_row(row)
    connection.commit()
    connection.close()
    entity.show()


class CommandEnd(argparse.Namespace):
    id: int
    end: Optional[str]


def command_end(args: CommandEnd):
    "End a time tracking entry"
    end = datetime.now().strftime(DB_DATE_FORMAT)
    if args.end is not None:
        end = parse_date_or_throw('end', args.end)
        end = end.strftime(DB_DATE_FORMAT)

    connection = sqlite3.connect(DB_PATH)
    cursor = get_cursor(connection)
    cursor.execute(
        'UPDATE timetrack SET end = ? WHERE rowid = ? '
        'RETURNING rowid, message, start, end, category',
        (end, args.id)
    )
    row = cursor.fetchone()
    entity = Timetracker.from_row(row)
    connection.commit()
    connection.close()
    entity.show()


class CommandDrop(argparse.Namespace):
    id: Optional[int]
    all: bool


def command_drop(args: CommandDrop):
    "Drop a time tracking entry"
    if args.id is None and not args.all:
        raise CommandError('No id given')

    if args.all:
        print('Deleting all')
        connection = sqlite3.connect(DB_PATH)
        cursor = get_cursor(connection)
        cursor.execute('DELETE FROM timetrack')
        count = cursor.rowcount
        connection.commit()
        connection.close()
        print(f'Deleted {count} rows')
    else:
        print('Deleting', args.id)
        connection = sqlite3.connect(DB_PATH)
        cursor = get_cursor(connection)
        cursor.execute('DELETE FROM timetrack WHERE rowid = ?', (args.id,))
        count = cursor.rowcount
        connection.commit()
        connection.close()
        print(f'Deleted {count} rows')


class CommandEdit(argparse.Namespace):
    id: int
    message: Optional[str]
    category: Optional[str]
    start: Optional[str]
    end: Optional[str]


def command_edit(args):
    "Edit a time tracking entry"
    connection = sqlite3.connect(DB_PATH)
    cursor = get_cursor(connection)
    cursor.execute('SELECT * FROM timetrack WHERE rowid = ?', (args.id,))
    row = cursor.fetchone()
    if row is None:
        raise CommandError(f'No row with id {args.id} found')

    fields = {'message': args.message, 'category': args.category,
              'start': args.start, 'end': args.end}
    fields = {k: v for k, v in fields.items() if v is not UNSET}
    if 'start' in fields:
        fields['start'] = parse_date_or_throw('start', fields['start'])
        fields['start'] = fields['start'].strftime(DB_DATE_FORMAT)
    if 'end' in fields:
        fields['end'] = parse_date_or_throw('end', fields['end'])
        fields['end'] = fields['end'].strftime(DB_DATE_FORMAT)
    if not fields:
        print('No changes given')
        return

    update = ', '.join(f'{k} = ?' for k in fields)
    values = [v for v in fields.values()]
    values.append(args.id)
    cursor.execute(
        f'UPDATE timetrack SET {update} WHERE rowid = ? '
        'RETURNING rowid, message, start, end, category',
        values
    )
    row = cursor.fetchone()
    entity = Timetracker.from_row(row)
    connection.commit()
    connection.close()
    entity.show()


class CommandList(argparse.Namespace):
    start: Optional[str]


def command_list(args: CommandList):
    "List time tracking entries"
    # TODO: Add ms formatter
    if args.start is None:
        start = datetime.now() - timedelta(hours=48)
        start = start.strftime(DB_DATE_FORMAT)
    elif args.start != 'all':
        start = parse_date_or_throw('start', args.start)
        start = start.strftime(DB_DATE_FORMAT)
    else:
        start = None

    connection = sqlite3.connect(DB_PATH)
    cursor = get_cursor(connection)

    rowid_len = 0
    cursor.execute('SELECT MAX(rowid) FROM timetrack')
    row = cursor.fetchone()
    if row:
        rowid_len = len(str(row[0]))

    if start:
        cursor.execute(
            'SELECT rowid, message, start, end, category '
            'FROM timetrack '
            'WHERE start >= ? '
            'ORDER BY start',
            (start,)
        )
    else:
        cursor.execute(
            'SELECT rowid, message, start, end, category '
            'FROM timetrack '
            'ORDER BY start'
        )

    now = datetime.now()
    for row in cursor:
        entity = Timetracker.from_row(row)
        entity.show(now, rowid_len)
    connection.close()


class CommandExport(argparse.Namespace):
    path: str
    format: Optional[str]


def command_export(args: CommandExport):
    "Export time tracking entries to 'format' file"

    connection = sqlite3.connect(DB_PATH)
    cursor = get_cursor(connection)
    cursor.execute(
        'SELECT rowid, start, end, category, message '
        'FROM timetrack '
        'ORDER BY start'
    )
    rows = cursor.fetchall()
    connection.close()

    out_format = args.format
    if out_format is None:
        out_format = Path(args.path).suffix[1:]

    if out_format == 'csv':
        with open(args.path, 'w') as f:
            print('start,end,category,message', file=f)
            for row in rows:
                print(f'{row[1]},{row[2] or ""},"{row[3]}","{row[4]}"', file=f)
        print(f'Exported {len(rows)} rows to {args.path}')

    elif out_format == 'json':
        with open(args.path, 'w') as f:
            json.dump([{
                'start': row[1],
                'end': row[2],
                'category': row[3],
                'message': row[4],
            } for row in rows], f)
        print(f'Exported {len(rows)} rows to {args.path}')


class CommandImport(argparse.Namespace):
    path: str
    format: Optional[str]


def command_import(args: CommandImport):
    "Import time tracking entries from 'format' file"
    in_format = args.format
    if in_format is None:
        in_format = Path(args.path).suffix[1:]

    if in_format == 'csv':
        data = []
        with open(args.path) as f:
            for i, line in enumerate(f):
                if i == 0:
                    header = line.strip().split(',')
                else:
                    row = line.strip().split(',')
                    data.append(dict(zip(header, row)))

    elif in_format == 'json':
        with open(args.path) as f:
            data = json.load(f)

    connection = sqlite3.connect(DB_PATH)
    cursor = get_cursor(connection)
    for batch in batched(data, 100):
        cursor.executemany(
            'INSERT INTO timetrack (start, end, category, message) '
            'VALUES (?, ?, ?, ?)',
            [(row['start'], row['end'], row['category'], row['message'])
                for row in batch]
        )
        connection.commit()
    connection.close()
    print(f'Imported {len(data)} rows from {args.path}')


class CommandMetrics(argparse.Namespace):
    start: Optional[str]
    end: Optional[str]


def command_metrics(args: CommandMetrics):
    "Show metrics"
    start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    end = None
    if args.start:
        start = parse_date_or_throw('start', args.start)
    if args.end:
        end = parse_date_or_throw('end', args.end)

    connection = sqlite3.connect(DB_PATH)
    cursor = get_cursor(connection)
    if end:
        cursor.execute(
            'SELECT category, start, end '
            'FROM timetrack '
            'WHERE start >= ? AND end <= ?',
            (start, end)
        )
    else:
        cursor.execute(
            'SELECT category, start, end '
            'FROM timetrack '
            'WHERE start >= ?',
            (start,)
        )
    rows = cursor.fetchall()
    rows = [(row[0], parse_date_db(row[1]), row[2] and parse_date_db(row[2]))
            for row in rows]
    cat_rows = [row for row in rows if row[0]]
    print(f'Total rows: {len(rows)}')
    print(
        f'Total time: {sum((row[2] - row[1] for row in rows if row[2]), timedelta())}')
    print(f'Total rows with category: {len(cat_rows)}')

    for category, category_rows in groupby(cat_rows, key=lambda row: row[0]):
        print(
            f'{category}: {sum((row[2] - row[1] for row in category_rows if row[2]), timedelta())}')


def get_parser():
    def command(func):
        name = func.__name__[len("command_"):].replace("_", "-")
        parser = subparsers.add_parser(name, help=func.__doc__)
        parser.set_defaults(func=func)
        return parser

    parser = argparse.ArgumentParser(description='Time tracker')
    subparsers = parser.add_subparsers(dest='command')

    sb = command(command_setup)
    sb.add_argument('--database-path', default=DB_PATH)

    sb = command(command_start)
    sb.add_argument('message', type=str)
    sb.add_argument('-c', '--category', type=str, default=None)
    sb.add_argument('-s', '--start', default=None)
    sb.add_argument('-e', '--end', default=None)

    sb = command(command_start_in)
    sb.add_argument('id', type=int)
    sb.add_argument('message', type=str)
    sb.add_argument('--category', default=None)

    sb = command(command_end)
    sb.add_argument('id', type=int)
    sb.add_argument('--end', default=None)

    sb = command(command_drop)
    sb.add_argument('id', type=int, default=None, nargs='?')
    sb.add_argument('--all', action='store_true')

    sb = command(command_edit)
    sb.add_argument('id', type=int)
    sb.add_argument('-m', '--message', type=str, default=UNSET)
    sb.add_argument('-c', '--category', type=str, default=UNSET)
    sb.add_argument('-s', '--start', default=UNSET)
    sb.add_argument('-e', '--end', default=UNSET)

    sb = command(command_list)
    sb.add_argument('--start', default=None)

    sb = command(command_export)
    sb.add_argument('path', type=str)
    sb.add_argument('--format', default=None, choices=['csv', 'json'])

    sb = command(command_import)
    sb.add_argument('path', type=str)
    sb.add_argument('--format', default=None, choices=['csv', 'json'])

    sb = command(command_metrics)
    sb.add_argument('--start', default=None)
    sb.add_argument('--end', default=None)

    return parser


def main():
    parser = get_parser()
    args = parser.parse_args()
    if args.command:
        args.func(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
