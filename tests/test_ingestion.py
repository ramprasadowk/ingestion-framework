import copy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from ingestion.config import load, validate
from ingestion.demo import seed, change
from ingestion.runner import run
from ingestion.sources import extract, TransientSourceError

ROOT=Path(__file__).resolve().parents[1]


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.config=load(ROOT/'config/local.json')
        seed(self.root)

    def rows(self,table):
        with sqlite3.connect(self.root/'runtime/bronze.db') as db:
            return db.execute('SELECT * FROM '+table+' ORDER BY 1').fetchall()

    def checkpoint(self):
        with sqlite3.connect(self.root/'runtime/bronze.db') as db:
            return db.execute("SELECT watermark FROM ingestion_checkpoints WHERE table_name='bronze_equipment'").fetchone()[0]

    def test_full_and_incremental_reruns_do_not_duplicate(self):
        for _ in range(2):
            self.assertTrue(all(r['status']=='SUCCEEDED' for r in run(self.config,self.root)))
        for t in self.config['tables']:
            self.assertEqual(len(self.rows(t['name'])),2)
        self.assertEqual(len(self.rows('ingestion_audit')),6)
        self.assertEqual(self.checkpoint(),2)

    def test_updated_record_and_watermark_tie(self):
        run(self.config,self.root)
        with sqlite3.connect(self.root/'runtime/source.db') as db:
            db.execute("INSERT INTO equipment VALUES(103,'Temporary Fan',25,2)")
        run(self.config,self.root)
        self.assertEqual(len(self.rows('bronze_equipment')),3)
        change(self.root)
        run(self.config,self.root)
        payloads=[json.loads(row[1]) for row in self.rows('bronze_equipment')]
        self.assertEqual(next(r['daily_rate'] for r in payloads if r['equipment_id']==101),50)
        self.assertEqual(self.checkpoint(),3)

    def test_failure_rolls_back_target_and_watermark_and_sanitizes_audit(self):
        run(self.config,self.root)
        before=self.rows('bronze_equipment')
        change(self.root)
        cfg=copy.deepcopy(self.config);cfg['tables']=[cfg['tables'][1]]
        def fail(db,table): raise RuntimeError('secret-password-must-not-appear')
        result=run(cfg,self.root,after_write=fail)
        self.assertEqual(result[0]['status'],'FAILED')
        self.assertEqual(self.rows('bronze_equipment'),before)
        self.assertEqual(self.checkpoint(),2)
        self.assertNotIn('secret-password',str(self.rows('ingestion_audit')))

    def test_empty_full_load_replaces_previous_snapshot(self):
        run(self.config,self.root)
        with sqlite3.connect(self.root/'runtime/source.db') as db: db.execute('DELETE FROM depots')
        run(self.config,self.root)
        self.assertEqual(self.rows('bronze_depots'),[])

    def test_row_limit_refuses_partial_checkpoint(self):
        cfg=copy.deepcopy(self.config);cfg['tables']=[cfg['tables'][1]];cfg['tables'][0]['max_rows']=1
        self.assertEqual(run(cfg,self.root)[0]['status'],'FAILED')
        with sqlite3.connect(self.root/'runtime/bronze.db') as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM ingestion_checkpoints').fetchone()[0],0)

    def test_only_transient_source_errors_retry_and_audit_each_attempt(self):
        cfg=copy.deepcopy(self.config);cfg['tables']=[cfg['tables'][1]]
        attempts=[]
        def flaky(*args):
            attempts.append(1)
            if len(attempts)==1: raise TransientSourceError('temporary')
            return extract(*args)
        self.assertEqual(run(cfg,self.root,extractor=flaky)[0]['status'],'SUCCEEDED')
        self.assertEqual(len(attempts),2)
        self.assertEqual(len(self.rows('ingestion_audit')),2)

    def test_duplicate_keys_fail_before_writing(self):
        with sqlite3.connect(self.root/'runtime/source.db') as db:
            db.execute('CREATE TABLE broken AS SELECT * FROM equipment')
            db.execute('INSERT INTO broken SELECT * FROM equipment')
        cfg=copy.deepcopy(self.config);cfg['tables']=[cfg['tables'][1]];cfg['tables'][0]['table']='broken'
        self.assertEqual(run(cfg,self.root)[0]['status'],'FAILED')

    def test_config_rejects_sql_identifiers_and_unknown_options(self):
        for field,value in [('table','equipment;DROP TABLE x'),('columns',['equipment_id','*'])]:
            cfg=copy.deepcopy(self.config);cfg['tables'][1][field]=value
            with self.assertRaises(ValueError): validate(cfg)
        cfg=copy.deepcopy(self.config);cfg['tables'][1]['password']='forbidden'
        with self.assertRaises(ValueError): validate(cfg)


if __name__=='__main__': unittest.main()
