"""Entirely synthetic equipment-rental data; no private reference content."""
import sqlite3
from pathlib import Path

SQL = '''
CREATE TABLE IF NOT EXISTS depots (depot_id INTEGER PRIMARY KEY, depot_name TEXT NOT NULL, region TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS equipment (equipment_id INTEGER PRIMARY KEY, equipment_name TEXT NOT NULL, daily_rate NUMERIC NOT NULL, change_version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS rental_orders (rental_id INTEGER PRIMARY KEY, equipment_id INTEGER NOT NULL, depot_id INTEGER NOT NULL, rental_days INTEGER NOT NULL, change_version INTEGER NOT NULL);
'''
ROWS = {
    'depots':[(1,'Cedar Depot','North'),(2,'Juniper Depot','South')],
    'equipment':[(101,'Compact Mixer',45,1),(102,'Portable Light Tower',75,2)],
    'rental_orders':[(9001,101,1,3,1),(9002,102,2,2,2)]
}


def seed(root):
    path=Path(root)/'runtime/source.db'
    path.parent.mkdir(parents=True,exist_ok=True)
    with sqlite3.connect(path) as db:
        db.executescript(SQL)
        for table,rows in ROWS.items():
            db.executemany('INSERT OR IGNORE INTO '+table+' VALUES('+','.join('?'*len(rows[0]))+')',rows)


def change(root):
    with sqlite3.connect(Path(root)/'runtime/source.db') as db:
        version=db.execute('SELECT MAX(change_version) FROM equipment').fetchone()[0]+1
        db.execute('UPDATE equipment SET daily_rate=daily_rate+5,change_version=? WHERE equipment_id=101',(version,))
