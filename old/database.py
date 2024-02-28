import sqlite3


def db_up(c: sqlite3.Cursor):
    c.execute(
        """
    CREATE TABLE IF NOT EXISTS timetrack (
        start DATETIME NOT NULL,
        message TEXT NOT NULL,
        end DATETIME,
        category TEXT
    )
    """
    )
    c.execute(
        """
    CREATE INDEX IF NOT EXISTS timetrack_start ON timetrack (start DESC)
    """
    )
    c.execute(
        """
    CREATE UNIQUE INDEX IF NOT EXISTS timetrack_start_message ON timetrack (start, message)
    """
    )
    c.execute('pragma encoding')


def db_down(c: sqlite3.Cursor):
    c.execute('DROP INDEX IF EXISTS timetrack_start')
    c.execute('DROP INDEX IF EXISTS timetrack_start_message')
    c.execute('DROP TABLE IF EXISTS timetrack')
