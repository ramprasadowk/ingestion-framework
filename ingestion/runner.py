"""Per-table atomic local writes, checkpoints and success audits."""
import json
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .config import quoted, validate
from .sources import extract, TransientSourceError


def now():
    return datetime.now(timezone.utc).isoformat()


def initialize(db):
    db.executescript('''
    CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
      table_name TEXT PRIMARY KEY, watermark INTEGER, updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ingestion_audit (
      run_id TEXT PRIMARY KEY, table_name TEXT NOT NULL, mode TEXT NOT NULL,
      attempt INTEGER NOT NULL, status TEXT NOT NULL,
      started_at TEXT NOT NULL, finished_at TEXT, rows_extracted INTEGER,
      watermark_before INTEGER, watermark_after INTEGER, error_code TEXT
    );
    ''')


def run(config, root='.', extractor=extract, after_write=None):
    validate(config)
    root = Path(root)
    destination = root / config['target_database']
    destination.parent.mkdir(parents=True, exist_ok=True)
    outcomes=[]
    with sqlite3.connect(destination, isolation_level=None, timeout=15) as db:
        initialize(db)
        for table in config['tables']:
            if not table.get('enabled',True):
                continue
            for attempt in range(1,table.get('retries',1)+2):
                run_id = str(uuid.uuid4())
                db.execute('INSERT INTO ingestion_audit(run_id,table_name,mode,attempt,status,started_at) VALUES(?,?,?,?,?,?)',
                           (run_id,table['name'],table['mode'],attempt,'STARTED',now()))
                try:
                    # Serialize local writers while reading checkpoint and applying the batch.
                    db.execute('BEGIN IMMEDIATE')
                    previous = db.execute('SELECT watermark FROM ingestion_checkpoints WHERE table_name=?',(table['name'],)).fetchone()
                    before = previous[0] if previous else None
                    rows, upper = extractor(config['sources'][table['source']],table,before,root)
                    target = quoted(table['name'])
                    db.execute('CREATE TABLE IF NOT EXISTS '+target+' (_record_key TEXT PRIMARY KEY, _payload TEXT NOT NULL, _run_id TEXT NOT NULL, _ingested_at TEXT NOT NULL)')
                    if table['mode'] == 'full':
                        db.execute('DELETE FROM '+target)
                    stamp=now()
                    for row in rows:
                        key=json.dumps([row[k] for k in table['keys']],default=str)
                        db.execute('INSERT INTO '+target+' VALUES(?,?,?,?) ON CONFLICT(_record_key) DO UPDATE SET _payload=excluded._payload,_run_id=excluded._run_id,_ingested_at=excluded._ingested_at',
                                   (key,json.dumps(row,default=str,sort_keys=True),run_id,stamp))
                    if after_write:
                        after_write(db, table)  # Fault-injection hook used by rollback tests.
                    if table['mode']=='incremental':
                        db.execute('INSERT INTO ingestion_checkpoints VALUES(?,?,?) ON CONFLICT(table_name) DO UPDATE SET watermark=excluded.watermark,updated_at=excluded.updated_at',
                                   (table['name'],upper,stamp))
                    db.execute("UPDATE ingestion_audit SET status='SUCCEEDED',finished_at=?,rows_extracted=?,watermark_before=?,watermark_after=? WHERE run_id=?",
                               (now(),len(rows),before,upper,run_id))
                    db.commit()
                    outcomes.append({'table':table['name'],'status':'SUCCEEDED','rows_extracted':len(rows),'run_id':run_id})
                    break
                except Exception as error:
                    if db.in_transaction:
                        db.rollback()
                    # Never persist raw exception messages, SQL or connection settings.
                    db.execute("UPDATE ingestion_audit SET status='FAILED',finished_at=?,error_code=? WHERE run_id=?",(now(),type(error).__name__,run_id))
                    if isinstance(error,TransientSourceError) and attempt<=table.get('retries',1):
                        time.sleep(min(.1*2**(attempt-1),1))
                        continue
                    outcomes.append({'table':table['name'],'status':'FAILED','error_code':type(error).__name__,'run_id':run_id})
                    break
    return outcomes
