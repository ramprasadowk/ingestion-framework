"""Source adapters; extraction never writes to the source database."""
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .config import quoted


class TransientSourceError(Exception):
    pass


@contextmanager
def connect(source, root):
    if source['kind'] == 'sqlite':
        path = (Path(root) / source['path']).resolve()
        connection = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
        connection.execute('BEGIN')
        try:
            yield connection, '?'
        finally:
            connection.close()
    else:
        import psycopg
        dsn = os.environ.get(source['dsn_env'])
        if not dsn:
            raise ValueError('Source DSN environment variable is missing.')
        try:
            with psycopg.connect(dsn, connect_timeout=15) as connection:
                connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                connection.execute("SET LOCAL statement_timeout = '60s'")
                yield connection, '%s'
        except psycopg.OperationalError:
            raise TransientSourceError('Postgres connection unavailable.') from None


def extract(source, table, last_watermark, root):
    """Read a bounded, consistent snapshot. Inclusive lower bound replays ties."""
    relation = quoted(table['table'])
    if source['kind'] == 'postgres':
        relation = quoted(table.get('schema','public')) + '.' + relation
    with connect(source, root) as (connection, placeholder):
        cursor = connection.cursor()
        upper = None
        parameters = []
        predicate = ''
        if table['mode'] == 'incremental':
            wm = quoted(table['watermark'])
            cursor.execute('SELECT COUNT(*) FROM ' + relation + ' WHERE ' + wm + ' IS NULL')
            if cursor.fetchone()[0]:
                raise ValueError('Watermark cannot be null.')
            cursor.execute('SELECT MAX(' + wm + ') FROM ' + relation)
            upper = cursor.fetchone()[0]
            if upper is not None and type(upper) is not int:
                raise ValueError('This example requires an integer change_version watermark.')
            if last_watermark is not None and (upper is None or upper < last_watermark):
                raise ValueError('Source watermark regressed; explicit reinitialization is required.')
            if upper is None:
                return [], last_watermark
            predicate = ' WHERE ' + wm + ' <= ' + placeholder
            parameters.append(upper)
            if last_watermark is not None:
                predicate += ' AND ' + wm + ' >= ' + placeholder
                parameters.append(last_watermark - table.get('lookback',0))
        limit = table.get('max_rows',50000)
        query = 'SELECT ' + ','.join(quoted(c) for c in table['columns']) + ' FROM ' + relation + predicate + ' LIMIT ' + placeholder
        cursor.execute(query, parameters + [limit + 1])
        rows = [dict(zip(table['columns'], values)) for values in cursor.fetchall()]
        if len(rows) > limit:
            raise ValueError('Batch exceeds max_rows; refusing to advance checkpoint. Partition or use distributed ingestion.')
        keys = set()
        for row in rows:
            key = tuple(row[k] for k in table['keys'])
            if any(v is None for v in key) or key in keys:
                raise ValueError('Source primary keys must be unique and non-null.')
            keys.add(key)
            if table['mode'] == 'incremental' and type(row[table['watermark']]) is not int:
                raise ValueError('Integer watermark required.')
        return rows, upper
