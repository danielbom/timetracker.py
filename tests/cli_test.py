from datetime import datetime
from cli import CommandDrop, CommandEdit, CommandEnd, CommandError, CommandList, CommandStart, CommandStartIn, batched, command_drop, command_edit, command_end, command_list, command_start, command_start_in
import pytest


@pytest.mark.parametrize("values, batch_size, expected", [
    ([1, 2, 3, 4, 5, 6, 7, 8], 3, [[1, 2, 3], [4, 5, 6], [7, 8]]),
    ([1, 2, 3, 4, 5, 6, 7, 8], 2, [[1, 2], [3, 4], [5, 6], [7, 8]]),
])
def test_iterate_over_batches(values, batch_size, expected):
    actual = list(batched(values, batch_size))
    assert actual == expected


class TestCommandStart:
    # Function called with valid arguments, start and end are None
    def test_valid_arguments_none_start_end(self, mocker):
        # Mock the necessary dependencies
        mocker.patch('sqlite3.connect')
        mocker.patch('cli.print')
        mocker.patch('cli.Timetracker.from_row')
        get_cursor_mock = mocker.patch('cli.get_cursor')
        datetime_mock = mocker.patch("cli.datetime")
        FAKE_NOW = datetime(2000, 1, 1, 12, 0, 0)
        datetime_mock.now.return_value = FAKE_NOW

        # Set up the test data
        args = CommandStart(
            message="Test message",
            category=None,
            start=None,
            end=None,
        )

        # Call the function under test
        command_start(args)

        # Assert that the correct SQL query was executed
        cursor_mock = get_cursor_mock.return_value
        cursor_mock.execute.assert_called_once_with(
            'INSERT INTO timetrack (message, start, end, category) '
            'VALUES (?, ?, ?, ?) '
            'RETURNING rowid, message, start, end, category',
            (args.message, "2000-01-01T12:00:00Z", None, args.category)
        )


class TestCommandStartIn:
    # start a new time tracking entry in the end of a completed entry
    def test_start_in_completed_entry(self, mocker):
        # Mock the necessary dependencies
        mocker.patch('sqlite3.connect')
        mocker.patch('cli.print')
        mocker.patch('cli.Timetracker.from_row')
        get_cursor_mock = mocker.patch('cli.get_cursor')

        # Set up the test data
        args = CommandStartIn(
            id=1,
            message="Test message",
            category=None,
        )

        # Mock the row returned from the SELECT query
        cursor_mock = get_cursor_mock.return_value
        cursor_mock.fetchone.return_value = ("2000-01-01T12:00:00Z",)

        # Call the function under test
        command_start_in(args)

        # Assert that the correct SQL query was executed
        cursor_mock.execute.assert_called_with(
            'INSERT INTO timetrack (message, start, end, category) '
            'VALUES (?, ?, ?, ?) '
            'RETURNING rowid, message, start, end, category',
            (args.message, "2000-01-01T12:00:00Z", None, args.category)
        )

    # raise an exception if no row with the given id is found
    def test_start_in_no_row_found(self, mocker):
        # Mock the necessary dependencies
        mocker.patch('sqlite3.connect')
        mocker.patch('cli.print')
        get_cursor_mock = mocker.patch('cli.get_cursor')

        # Set up the test data
        args = CommandStartIn(
            id=1,
            message="Test message",
            category=None,
        )

        # Mock the row returned from the SELECT query
        cursor_mock = get_cursor_mock.return_value
        cursor_mock.fetchone.return_value = None

        # Call the function under test and assert that it raises the expected exception
        with pytest.raises(CommandError) as exc_info:
            command_start_in(args)

        # Assert that the correct exception message is raised
        assert str(exc_info.value) == f'No row with id {args.id} found'


class TestCommandEnd:
    # Ends a time tracking entry with current time if no end time is provided
    def test_end_entry_with_current_time(self, mocker):
        # Mock the necessary dependencies
        mocker.patch('sqlite3.connect')
        mocker.patch('cli.print')
        mocker.patch('cli.Timetracker.from_row')
        get_cursor_mock = mocker.patch('cli.get_cursor')
        datetime_mock = mocker.patch("cli.datetime")
        FAKE_NOW = datetime(2000, 1, 1, 12, 0, 0)
        datetime_mock.now.return_value = FAKE_NOW

        # Set up the test data
        args = CommandEnd(id=1, end=None)

        # Call the function under test
        command_end(args)

        # Assert that the correct SQL query was executed
        cursor_mock = get_cursor_mock.return_value
        cursor_mock.execute.assert_called_once_with(
            'UPDATE timetrack SET end = ? WHERE rowid = ? '
            'RETURNING rowid, message, start, end, category',
            ("2000-01-01T12:00:00Z", args.id)
        )


class TestCommandDrop:
    # Deletes a time tracking entry with a given id
    def test_delete_entry_with_id(self, mocker):
        # Mock the necessary dependencies
        mocker.patch('sqlite3.connect')
        mocker.patch('cli.print')
        get_cursor_mock = mocker.patch('cli.get_cursor')

        # Set up the test data
        args = CommandDrop(id=1, all=False)

        # Call the function under test
        command_drop(args)

        # Assert that the correct SQL query was executed
        cursor_mock = get_cursor_mock.return_value
        cursor_mock.execute.assert_called_once_with(
            'DELETE FROM timetrack WHERE rowid = ?', (args.id,)
        )


class TestCommandEdit:
    # edits an existing time tracking entry with valid input
    def test_edit_existing_entry_valid_input(self, mocker):
        # Mock the necessary dependencies
        mocker.patch('sqlite3.connect')
        mocker.patch('cli.print')
        mocker.patch('cli.Timetracker.from_row')
        get_cursor_mock = mocker.patch('cli.get_cursor')
        mocker.patch('cli.DB_PATH', 'test_db_path')

        # Set up the test data
        args = CommandEdit(
            id=1,
            message='New message',
            category='New category',
            start='2022-01-01',
            end='2022-01-02'
        )

        # Call the function under test
        command_edit(args)

        # Assert that the correct SQL query was executed
        cursor_mock = get_cursor_mock.return_value
        cursor_mock.execute.mock_calls[1](
            'UPDATE timetrack SET message = ?, category = ?, start = ?, end = ? WHERE rowid = ? '
            'RETURNING rowid, message, start, end, category',
            ['New message', 'New category',
                '2022-01-01T00:00:00Z', '2022-01-02T00:00:00Z', 1]
        )


class TestCommandList:
    # List time tracking entries when no start date is provided
    def test_start_all(self, mocker):
        # Mock the necessary dependencies
        mocker.patch('sqlite3.connect')
        mocker.patch('cli.print')
        get_cursor_mock = mocker.patch('cli.get_cursor')
        datetime_mock = mocker.patch("cli.datetime")
        FAKE_NOW = datetime(2000, 1, 1, 12, 0, 0)
        datetime_mock.now.return_value = FAKE_NOW

        # Set up the test data
        args = CommandList(start='all')

        # Call the function under test
        command_list(args)

        # Assert that the correct SQL query was executed
        cursor_mock = get_cursor_mock.return_value
        cursor_mock.execute.mock_calls[1](
            'SELECT rowid, message, start, end, category '
            'FROM timetrack '
            'ORDER BY start',
        )

    # List all time tracking entries when no start date is provided

    def test_start_undefined(self, mocker):
        # Mock the necessary dependencies
        mocker.patch('sqlite3.connect')
        mocker.patch('cli.print')
        get_cursor_mock = mocker.patch('cli.get_cursor')
        datetime_mock = mocker.patch("cli.datetime")
        FAKE_NOW = datetime(2000, 1, 1, 12, 0, 0)
        datetime_mock.now.return_value = FAKE_NOW

        # Set up the test data
        args = CommandList(start=None)

        # Call the function under test
        command_list(args)

        # Assert that the correct SQL query was executed
        cursor_mock = get_cursor_mock.return_value
        cursor_mock.execute.mock_calls[1](
            'SELECT rowid, message, start, end, category '
            'FROM timetrack '
            'WHERE start >= ? '
            'ORDER BY start',
            ("1999-12-30T12:00:00Z",)
        )

    def test_start_defined(self, mocker):
        # Mock the necessary dependencies
        mocker.patch('sqlite3.connect')
        mocker.patch('cli.print')
        get_cursor_mock = mocker.patch('cli.get_cursor')

        # Set up the test data
        args = CommandList(start='2099-01-01')

        # Call the function under test
        command_list(args)

        # Assert that the correct SQL query was executed
        cursor_mock = get_cursor_mock.return_value
        cursor_mock.execute.mock_calls[1](
            'SELECT rowid, message, start, end, category '
            'FROM timetrack '
            'WHERE start >= ? '
            'ORDER BY start',
            ('2099-01-01T00:00:00Z',)
        )
