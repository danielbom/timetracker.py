from datetime import datetime, timedelta
from itertools import groupby
import sys
import traceback
from commands import command_end, command_from_csv, command_list, command_restart, command_start, command_start_in, command_to_csv
from constants import CLI_DATE_FORMAT, CLI_HOUR_FORMAT, CLI_PRINT_DATE_FORMAT
from core import Row
from repositories import TimetrackerRepository


def normalize_durantion(duration: timedelta):
    return duration - timedelta(microseconds=duration.microseconds)


def _sum_timedelta(iterable):
    return sum(iterable, timedelta(0))


def _format_duration(duration: timedelta):
    hours, remainder = divmod(duration.total_seconds(), 3600)
    minutes, _seconds = divmod(remainder, 60)
    return f"{int(hours):02d}:{int(minutes):02d}"


def _print_row(row: Row):
    # https://stackoverflow.com/questions/31018497/how-to-format-duration-in-python-timedelta
    start = row.start.strftime(CLI_PRINT_DATE_FORMAT)
    end = row.end.strftime(CLI_PRINT_DATE_FORMAT) if row.end else (" " * 15)
    duration = _format_duration(row.start - (row.end or datetime.now()))
    print(f"{start} .. {end} | {duration} | {row.message}")


def cmd_list(args):
    now = datetime.now()
    rows = command_list(*args)

    max_rowid = TimetrackerRepository.max_rowid()
    rowid_len = len(str(max_rowid))

    for row in rows:
        start_day = row.start.strftime(CLI_DATE_FORMAT)
        start = row.start.strftime(CLI_HOUR_FORMAT)
        end = row.end.strftime(CLI_HOUR_FORMAT) if row.end else "--:--"
        duration = _format_duration((row.end or now) - row.start)
        rowid = str(row.rowid).rjust(rowid_len)
        print(f"{rowid}: {start_day} | {start} .. {end} | {duration} | {row.message}")


def cmd_start(args):
    new_row = command_start(*args)
    _print_row(new_row)


def cmd_start_in(args):
    if len(args) < 1:
        print("Error: No id given")
        exit(1)

    new_row = command_start_in(*args)
    _print_row(new_row)


def cmd_restart(args):
    if len(args) < 1:
        print("Error: No id given")
        exit(1)

    new_row = command_restart(*args)
    _print_row(new_row)


def cmd_end(args):
    if len(args) < 1:
        print("Error: No id given")
        exit(1)

    row = command_end(*args)
    _print_row(row)


def cmd_to_csv(args):
    command_to_csv(args[0] if len(args) > 0 else "")
    print("Done")


def cmd_from_csv(args):
    command_from_csv(args[0] if len(args) > 0 else "")
    print("Done")


def cmd_metrics(args):
    # TODO: Improve this metrics
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    rows = TimetrackerRepository.find_many(today)
    print(f"Total rows: {len(rows)}")
    print(f"Total time: {_sum_timedelta(row.duration for row in rows)}")

    rows_with_category = [row for row in rows if row.category]
    print(f"Total rows with category: {len(rows_with_category)}")

    for category, category_rows in groupby(rows_with_category, key=lambda row: row.category):
        print(f"{category}: {_sum_timedelta(row.duration for row in category_rows)}")


def cmd_help():
    commands = [
        ("start <message> <category?> <start?> <end?>", "Start a new row"),
        ("start-in <id> '<message> <category?>'",
         "Start a new row in the end of a row"),
        ("restart <id>", "Restart a row"),
        ("end <id>", "End a row"),
        ("list <date?>", "List all rows"),
        ("to-csv", "Export to CSV"),
        ("from-csv", "Import from CSV"),
        ("metrics", "Show metrics"),
        ("help", "Show this help"),
    ]
    print("Usage: cli.py [command]")
    print("Commands:")
    print("")
    for command, description in commands:
        print(f"  {command:20} {description}")


if __name__ == "__main__":
    args = sys.argv[1:]
    cmd = globals().get(f"cmd_{args[0].replace('-', '_')}") if args else None
    if cmd:
        try:
            cmd(args[1:])
        except Exception as e:
            print("Error:", e)
            traceback.print_exc()
            exit(1)
    else:
        cmd_help()
