import argparse
import json
import sqlite3
from pathlib import Path

from .config import load
from .demo import seed, change
from .runner import run


def main():
    parser=argparse.ArgumentParser(description='Reusable, configuration-driven ingestion demo')
    parser.add_argument('command',choices=['seed','change','run','audit','validate'])
    parser.add_argument('--config',default='config/local.json')
    args=parser.parse_args()
    root=Path.cwd()
    if args.command=='seed':
        seed(root); print('Synthetic source tables ready.'); return
    if args.command=='change':
        change(root); print('Synthetic equipment change added.'); return
    try:
        config=load(args.config)
        if args.command=='validate':
            print('Configuration valid.'); return
        if args.command=='audit':
            with sqlite3.connect(root/config['target_database']) as db:
                db.row_factory=sqlite3.Row
                results=[dict(row) for row in db.execute('SELECT * FROM ingestion_audit ORDER BY started_at')]
            print(json.dumps(results,indent=2)); return
        outcomes=run(config,root)
        print(json.dumps(outcomes,indent=2))
        if any(row['status']=='FAILED' for row in outcomes):
            raise SystemExit(1)
    except (ValueError,KeyError,sqlite3.Error):
        raise SystemExit('Configuration or local runtime error. Validate the config and seed the demo; details withheld.') from None


if __name__=='__main__':
    main()
