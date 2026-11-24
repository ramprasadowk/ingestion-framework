"""Optional: create fictional source tables in your own PostgreSQL database."""
import argparse
import os
from .demo import ROWS


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply',action='store_true',required=True,help='Explicitly create the rental_demo schema and seed synthetic rows.')
    parser.parse_args()
    import psycopg
    from psycopg import sql
    dsn=os.environ.get('PG_SETUP_DSN')
    if not dsn:
        raise SystemExit('Set PG_SETUP_DSN privately first.')
    try:
        with psycopg.connect(dsn,connect_timeout=15) as db:
            db.execute('CREATE SCHEMA IF NOT EXISTS rental_demo')
            db.execute('CREATE TABLE IF NOT EXISTS rental_demo.depots(depot_id BIGINT PRIMARY KEY,depot_name TEXT NOT NULL,region TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS rental_demo.equipment(equipment_id BIGINT PRIMARY KEY,equipment_name TEXT NOT NULL,daily_rate NUMERIC(12,2) NOT NULL,change_version BIGINT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS rental_demo.rental_orders(rental_id BIGINT PRIMARY KEY,equipment_id BIGINT NOT NULL,depot_id BIGINT NOT NULL,rental_days INTEGER NOT NULL,change_version BIGINT NOT NULL)')
            for table,rows in ROWS.items():
                statement=sql.SQL('INSERT INTO rental_demo.{} VALUES ({}) ON CONFLICT DO NOTHING').format(sql.Identifier(table),sql.SQL(',').join(sql.Placeholder() for _ in rows[0]))
                with db.cursor() as cursor:
                    cursor.executemany(statement,rows)
        print('Fictional rental_demo source ready. Grant a separate source login SELECT access before ingestion.')
    except Exception:
        raise SystemExit('Source setup failed; details withheld to protect credentials.') from None


if __name__=='__main__': main()
