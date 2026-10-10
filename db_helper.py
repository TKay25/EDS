"""
Database Helper Module for EDSDEVV
Provides context manager for safe database connection handling
"""

from contextlib import contextmanager
import atexit
import threading
import psycopg2
import psycopg2.pool
import os

# Get database URL from environment or fallback to default
external_database_url = os.getenv(
    'DATABASE_URL',
    "postgresql://lmsdatabase_8ag3_user:6WD9lOnHkiU7utlUUjT88m4XgEYQMTLb@dpg-ctp9h0aj1k6c739h9di0-a.oregon-postgres.render.com/lmsdatabase_8ag3"
)



# The database is remote, so opening a connection costs more than the query it
# carries (measured at ~1.7s from Zimbabwe to Render's Oregon region). A pool
# means that handshake is paid once per process instead of once per call.
_POOL_MIN = int(os.getenv('DB_POOL_MIN', '1'))
_POOL_MAX = int(os.getenv('DB_POOL_MAX', '8'))

_pool = None
_pool_lock = threading.Lock()


def _get_pool():
    """The shared connection pool, created on first use.

    One pool per process: psycopg2 connections cannot be shared across
    processes, and gunicorn pre-forks its workers.
    """
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is None:
            _pool = psycopg2.pool.ThreadedConnectionPool(
                _POOL_MIN, _POOL_MAX, external_database_url
            )
    return _pool


def _close_pool():
    """Close every pooled connection at interpreter exit."""
    global _pool
    if _pool is not None:
        try:
            _pool.closeall()
        except Exception:
            pass
        _pool = None


atexit.register(_close_pool)


def _discard(connection, pool):
    """Drop a connection that may be broken rather than returning it."""
    try:
        pool.putconn(connection, close=True)
    except Exception:
        try:
            connection.close()
        except Exception:
            pass


@contextmanager
def get_db():
    """
    Context manager for database connections.

    Yields a cursor on a pooled connection. The connection is handed back to the
    pool afterwards instead of being closed, so the next caller skips the
    connection handshake.

    Usage:
        with get_db() as (cursor, connection):
            cursor.execute("SELECT * FROM table WHERE id = %s", (123,))
            result = cursor.fetchone()
            connection.commit()

    Guarantees:
    - The connection is returned to the pool (finally block)
    - The cursor is closed after use (finally block)
    - No connection leaks on exceptions
    - An uncommitted transaction is rolled back before reuse
    """
    connection = None
    cursor = None
    pool = None
    try:
        pool = _get_pool()
        connection = pool.getconn()
        cursor = connection.cursor()
        yield cursor, connection
    except Exception:
        if connection:
            try:
                connection.rollback()
            except Exception:
                if pool:
                    _discard(connection, pool)
                    connection = None
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass  # Ignore errors during cursor close
        if connection and pool:
            try:
                # A pooled connection may still hold an open transaction, so it
                # is rolled back before the next caller receives it. Anything the
                # caller wanted kept was already committed by the caller.
                if connection.get_transaction_status() != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
                    connection.rollback()
                pool.putconn(connection)
            except Exception:
                _discard(connection, pool)


@contextmanager
def get_db_cursor_only():
    """
    Simplified context manager that yields only cursor.

    Usage:
        with get_db_cursor_only() as cursor:
            cursor.execute("SELECT * FROM table")
            results = cursor.fetchall()

    Note: Remember to manually commit if doing writes!
    """
    with get_db() as (cursor, _connection):
        yield cursor
