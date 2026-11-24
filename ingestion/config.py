"""Validate configuration before any connection or write is attempted."""
import json
import re
from pathlib import Path


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', value):
        raise ValueError('Identifiers must contain only letters, digits and underscores.')
    return value


def quoted(value):
    return '"' + identifier(value) + '"'


def validate(config):
    if set(config) - {'version', 'target_database', 'sources', 'tables'} or config.get('version') != 1:
        raise ValueError('Unsupported configuration version or fields.')
    if not isinstance(config.get('target_database'), str) or not config['target_database']:
        raise ValueError('target_database is required.')
    if not isinstance(config.get('sources'), dict) or not config['sources']:
        raise ValueError('At least one named source is required.')
    for name, source in config['sources'].items():
        identifier(name)
        if source.get('kind') == 'sqlite':
            if set(source) != {'kind', 'path'} or not source.get('path'):
                raise ValueError('SQLite source requires kind and path only.')
        elif source.get('kind') == 'postgres':
            if set(source) != {'kind', 'dsn_env'}:
                raise ValueError('Postgres source requires an environment reference, not credentials.')
            identifier(source['dsn_env'])
        else:
            raise ValueError('Unsupported connector; add an adapter for a new source technology.')
    tables = config.get('tables')
    if not isinstance(tables, list) or not tables:
        raise ValueError('Configure at least one table.')
    names = set()
    for t in tables:
        if set(t) - {'name','source','schema','table','columns','keys','mode','watermark','lookback','max_rows','retries','enabled'}:
            raise ValueError('Unknown table option.')
        name = identifier(t['name'])
        if name in names or name in {'ingestion_audit', 'ingestion_checkpoints'}:
            raise ValueError('Duplicate or reserved target name.')
        names.add(name)
        identifier(t['table']); identifier(t.get('schema', 'public'))
        if t['source'] not in config['sources'] or t['mode'] not in ('full', 'incremental'):
            raise ValueError('Unknown source or load mode.')
        for field in ('columns', 'keys'):
            values = t.get(field)
            if not isinstance(values, list) or not values or len(values) != len(set(values)):
                raise ValueError('Explicit, unique columns and keys are required.')
            for value in values: identifier(value)
        if not set(t['keys']).issubset(t['columns']):
            raise ValueError('Keys must be included in columns.')
        if t['mode'] == 'incremental' and t.get('watermark') not in t['columns']:
            raise ValueError('Incremental loads require a selected integer watermark column.')
        for option, default, minimum, maximum in [('max_rows',50000,1,1000000),('retries',1,0,5),('lookback',0,0,1000000)]:
            v=t.get(option,default)
            if type(v) is not int or not minimum <= v <= maximum:
                raise ValueError('Invalid bounded load option.')
        if 'enabled' in t and type(t['enabled']) is not bool:
            raise ValueError('enabled must be boolean.')
    return config


def load(path):
    return validate(json.loads(Path(path).read_text()))
