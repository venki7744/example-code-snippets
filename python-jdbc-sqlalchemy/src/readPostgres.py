import sqlalchemy
import jaydebeapi
#import sqlalchemy-jdbcapi
import pandas as pd
import os
import datetime
from sqlalchemy import event
import io
from  gzip import GzipFile


def read_sql_inmem_gzip_pandas_decompress(query, db_engine):
    copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(
       query=query, head="HEADER"
    )
    conn = db_engine.raw_connection()
    cur = conn.cursor()
    store = io.BytesIO()
    with GzipFile(fileobj=store, mode='w') as out:
        cur.copy_from(copy_sql, out)
    store.seek(0)
    df = pd.read_csv(store, compression='gzip')
    return df

print(os.environ["PG_JDBC_DRIVER_PATH"])#="D:\Learn\ApacheBeam\jar\postgresql-42.2.16.jar"

con=sqlalchemy.create_engine("jdbcapi+pgjdbc://{}:{}@{}/{}".format('postgres', 'postgres', 'localhost', 'sqlalchemyTest')).execution_options(isolation_level='AUTOCOMMIT')
wcon=sqlalchemy.create_engine("jdbcapi+pgjdbc://{}:{}@{}/{}".format('postgres', 'postgres', 'localhost', 'sqlalchemyTest'))
@event.listens_for(wcon,"before_cursor_execute")
def receive_before_cursor_execute(
       conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True

print('starttime-{}'.format(datetime.datetime.now()))
#for readChunk in pd.read_sql("select randomText,randomNumber from public.testtable",con,chunksize=100000):
#readChunk=pd.read_sql("select randomText,randomNumber from public.testtable",con,index_col=None,chunksize=None)
#print(readChunk)
    #print("count:{}".format(len(readChunk)))
    #readChunk.to_sql('testtableout1',wcon,schema='public',if_exists='append',index=False)
readChunk=read_sql_inmem_gzip_pandas_decompress("select randomText,randomNumber from public.testtable",con)
readChunk.to_sql('testtableout1',wcon,schema='public',if_exists='append',index=False)
print('Endtime-{}'.format(datetime.datetime.now()))