from datetime import datetime
from typing import Optional

from core import Row, get_data_file, store, store_message

CONTINUE = 0
BREAK = 1
PAGE_SIZE = 5
INPUT = '=> '
MESSAGE_CONFIRM = 'Are you sure you want to save this message? [y/N] '
MIN_MESSAGE_LENGTH = 10


class BaseApp:
    def _input(self) -> str:
        return input(INPUT)

    def _print(self, *args, **kwargs) -> None:
        print('  ', *args, **kwargs)

    def log_info(self, *args, **kargs) -> None:
        print('[INFO]', *args, **kargs)

    def log_warn(self, *args, **kargs) -> None:
        print('[WARN]', *args, **kargs)

    def log_error(self, *args, **kargs) -> None:
        print('[ERROR]', *args, **kargs)


class ChooseLine(BaseApp):
    running: bool = False
    result: Optional[str] = None

    def run(self):
        data_file = get_data_file()

        with open(data_file, 'r') as f:
            lines = f.readlines()

        lines = list(reversed(lines))

        take = PAGE_SIZE
        take_len = len(str(take))
        lines_len = len(lines)
        self.running = True
        while self.running:
            page = 0
            skip = page * take
            rows = [Row.parse(it) for it in lines[skip : skip + take]]
            max_value = len(rows)

            if skip > lines_len:
                self._print('No more messages')
                break

            for i, row in enumerate(rows):
                category = '- None -' if not row.category else row.category
                self._print(
                    f'{i: {take_len}d} -> {row.start} [{category}] : {row.message}'
                )

            self._print('Choose some messages to repeat')
            value = -1
            while not self.is_value_valid(value, max_value):
                try:
                    maybe_value = self._input()

                    if maybe_value == '':
                        continue
                    if maybe_value == 'q':
                        self.running = False
                        break
                    if maybe_value == 'n':
                        break

                    value = int(maybe_value)
                except ValueError:
                    self.log_error('Invalid value')
                    continue

            if not self.running:
                break

            if value != -1:
                self.result = rows[value].message
                break

            page += 1

    def is_value_valid(self, value, max_value):
        return value >= 0 and value < max_value


class App(BaseApp):
    message: str = ''
    last_message: Optional[str] = None
    running: bool = False

    def _help(self):
        self._print('Type your message and press enter to track.')
        self._print('Type :add to add a message with date and time.')
        self._print('Type :repeat to repeat some last message.')
        self._print('Type :quit to quit.')
        self._print('Type :start to add a start message.')
        self._print('Type :end to add a end message.')
        self._print('Type :help to see this help message.')

    def execute_command(self) -> int:
        message = self.message

        # :quit
        if message.startswith(':q'):
            self.running = False
            return BREAK

        # :add
        if message.startwith(':a'):
            self._print('Is today? [Y/n]')
            is_today = self._input()
            if is_today.lower() == 'n':
                start = None
                while start is None:
                    try:
                        self._print('Date? [YYYY-MM-DD]')
                        start = self._input()
                        if start == ':q':
                            return BREAK

                        start = datetime.strptime(start, '%Y-%m-%d')
                    except ValueError:
                        self._print('Invalid date. Try again.')
                        continue
            else:
                start = datetime.now()

            time = None
            while time is None:
                try:
                    self._print('Time? [HH:MM]')
                    time = self._input()
                    if time == ':q':
                        return BREAK

                    time = datetime.strptime(time, '%H:%M')
                except ValueError:
                    self._print('Invalid time. Try again.')
                    continue

            self._print('Message?')
            message = self._input()

            start = start.replace(
                hour=time.hour, minute=time.minute, second=0, microsecond=0
            )
            store(Row(start, message))
            return BREAK

        # :repeat
        if message.startswith(':r'):
            choose_line = ChooseLine()
            choose_line.run()
            if choose_line.result is not None:
                self.message = choose_line.result
                self._print('Message repeated.')
                return CONTINUE

            return BREAK

        # :help
        if message.startswith(':h'):
            self._help()
            return BREAK

        self.log_error('Unknown command: ' + message)
        return BREAK

    def try_command(self):
        if self.message.startswith(':'):
            return self.execute_command()
        else:
            return CONTINUE

    def check_message(self):
        if self.message == '!':
            self.message = self.last_message
            self._print('Last message repeated.')
            return CONTINUE

        if len(self.message) < MIN_MESSAGE_LENGTH:
            if self.message[-1] == '!':
                self.message = self.message[:-1]
                return CONTINUE

            self.log_warn('Message too short')
            self._print(MESSAGE_CONFIRM)
            confirm = self._input()
            if confirm.lower() != 'y':
                self.last_message = self.message
                self._print('Message discarded')
                return BREAK

        return CONTINUE

    def store_message(self):
        store_message(self.message)
        self.last_message = self.message
        self.log_info(f'Message saved: {self.message}')

    def execute(self):
        self.message = self._input()

        if self.try_command() == BREAK:
            return

        if self.check_message() == BREAK:
            return

        self.store_message()

    def run(self):
        self.running = True
        self._print('TimeTracker is running. Type :h for help.')
        while self.running:
            self.execute()


if __name__ == '__main__':
    app = App()
    app.run()
