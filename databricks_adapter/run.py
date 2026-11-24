"""Distributed JDBC -> Delta loading. Run on Databricks, not the local demo.

The target write and control-table writes are separate transactions. Keyed MERGE
makes a replay safe after a target commit but before a checkpoint commit.
Configure the Databricks Job for ONE concurrent run for this configuration.
"""
import json
import os
import uuid
from datetime import datetime,timezone
from pathlib import Path

from ingestion.config import identifier, load, quoted


def now(): return datetime.now(timezone.utc)


def run(spark, settings_path='config/databricks.json'):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    settings=json.loads(Path(settings_path).read_text())
    config=load(settings['ingestion_config'])
    catalog=identifier(settings['catalog'])
    bronze=identifier(settings['bronze_schema'])
    control=identifier(settings['control_schema'])
    fetch_size=settings.get('fetch_size',5000)
    if type(fetch_size) is not int or not 1<=fetch_size<=100000:
        raise ValueError('Invalid JDBC fetch size.')
    # Catalog must already exist and be accessible to the job service principal.
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze}`')
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{control}`')
    checkpoints=f'{catalog}.{control}.watermarks'
    audits=f'{catalog}.{control}.audit_runs'
    spark.sql(f'CREATE TABLE IF NOT EXISTS {checkpoints} (table_name STRING,watermark BIGINT,updated_at TIMESTAMP) USING DELTA')
    spark.sql(f'CREATE TABLE IF NOT EXISTS {audits} (run_id STRING,table_name STRING,mode STRING,status STRING,started_at TIMESTAMP,finished_at TIMESTAMP,rows_extracted BIGINT,watermark_before BIGINT,watermark_after BIGINT,error_code STRING) USING DELTA')
    audit_delta=DeltaTable.forName(spark,audits)
    def read(query,source_name):
        prefix=identifier(settings['jdbc_sources'][source_name])
        required=[prefix+'_JDBC_URL',prefix+'_JDBC_USER',prefix+'_JDBC_PASSWORD']
        if not all(os.environ.get(key) for key in required):
            raise ValueError('JDBC secret environment variables are missing.')
        url,user,password=[os.environ[key] for key in required]
        if not url.startswith('jdbc:postgresql:') or any(key in url.lower() for key in ('password=','user=')):
            raise ValueError('Use a PostgreSQL JDBC URL without embedded credentials.')
        return (spark.read.format('jdbc').option('url',url)
                .option('driver','org.postgresql.Driver').option('user',user).option('password',password)
                .option('dbtable','('+query+') AS source_batch').option('fetchsize',fetch_size).option('queryTimeout',120).load())
    results=[]
    for table in config['tables']:
        if not table.get('enabled',True): continue
        if config['sources'][table['source']]['kind']!='postgres':
            raise ValueError('This adapter implements PostgreSQL only.')
        run_id=str(uuid.uuid4()); data=None; cached=None; before=None; upper=None
        audit_schema='run_id string,table_name string,mode string,status string,started_at timestamp,finished_at timestamp,rows_extracted long,watermark_before long,watermark_after long,error_code string'
        spark.createDataFrame([(run_id,table['name'],table['mode'],'STARTED',now(),None,None,None,None,None)],audit_schema).write.mode('append').saveAsTable(audits)
        try:
            saved=spark.table(checkpoints).where(F.col('table_name')==table['name']).collect()
            if len(saved)>1: raise ValueError('Duplicate checkpoint; serialize jobs and repair control state.')
            before=saved[0]['watermark'] if saved else None
            relation=quoted(table.get('schema','public'))+'.'+quoted(table['table'])
            where=''
            if table['mode']=='incremental':
                wm=quoted(table['watermark'])
                stats=read(f'SELECT MAX({wm}) AS high_watermark,COUNT(*) FILTER (WHERE {wm} IS NULL) AS null_watermarks FROM {relation}',table['source']).first()
                upper=stats['high_watermark']
                if stats['null_watermarks'] or upper is not None and type(upper) is not int:
                    raise ValueError('Non-null BIGINT watermark required.')
                if before is not None and (upper is None or upper<before):
                    raise ValueError('Watermark regression detected.')
                where=' WHERE 1=0' if upper is None else f' WHERE {wm} <= {int(upper)}'
                if before is not None and upper is not None:
                    where+=f' AND {wm} >= {int(before-table.get("lookback",0))}'
            query='SELECT '+','.join(quoted(c) for c in table['columns'])+' FROM '+relation+where
            cached=read(query,table['source']).persist()
            data=cached
            count=data.count()
            if count>table.get('max_rows',50000): raise ValueError('Batch exceeds configured row limit.')
            bad=None
            for key in table['keys']:
                predicate=F.col(key).isNull();bad=predicate if bad is None else bad|predicate
            if data.where(bad).limit(1).count() or data.groupBy(*table['keys']).count().where('count > 1').limit(1).count():
                raise ValueError('Keys must be non-null and unique before MERGE.')
            data=data.withColumn('_ingest_run_id',F.lit(run_id)).withColumn('_ingested_at',F.lit(now()))
            target=f'{catalog}.{bronze}.{table["name"]}'
            if table['mode']=='full':
                data.write.format('delta').mode('overwrite').saveAsTable(target)
            elif not spark.catalog.tableExists(target):
                data.write.format('delta').mode('errorifexists').saveAsTable(target)
            elif count:
                condition=' AND '.join(f't.`{key}` = s.`{key}`' for key in table['keys'])
                wm=identifier(table['watermark'])
                (DeltaTable.forName(spark,target).alias('t').merge(data.alias('s'),condition)
                 .whenMatchedUpdateAll(condition=f's.`{wm}` >= t.`{wm}`').whenNotMatchedInsertAll().execute())
            if table['mode']=='incremental':
                checkpoint=spark.createDataFrame([(table['name'],upper if upper is not None else before,now())],'table_name string,watermark long,updated_at timestamp')
                (DeltaTable.forName(spark,checkpoints).alias('t').merge(checkpoint.alias('s'),'t.table_name=s.table_name')
                 .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
            audit_delta.update(F.col('run_id')==run_id,{'status':F.lit('SUCCEEDED'),'finished_at':F.lit(now()),'rows_extracted':F.lit(count),'watermark_before':F.lit(before).cast('long'),'watermark_after':F.lit(upper).cast('long')})
            results.append({'table':table['name'],'rows_extracted':count,'status':'SUCCEEDED'})
        except Exception as error:
            try:
                audit_delta.update(F.col('run_id')==run_id,{'status':F.lit('FAILED'),'finished_at':F.lit(now()),'error_code':F.lit(type(error).__name__)})
            except Exception:
                pass  # Audit may remain STARTED if control storage is unavailable; reconcile operationally.
            raise RuntimeError('Ingestion failed for '+table['name']+'; consult sanitized audit status.') from None
        finally:
            if cached is not None: cached.unpersist()
    return results
