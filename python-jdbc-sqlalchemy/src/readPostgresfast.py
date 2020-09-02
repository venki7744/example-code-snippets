import sqlalchemy
import jaydebeapi
#import sqlalchemy-jdbcapi
import pandas as pd
import os
import datetime
from sqlalchemy import event
import io
from  gzip import GzipFile
from pyarrow import jvm
import jpype
import sys
import jaydebeapi


def read_sql_pyarrow(query, db_details):
    from jpype import imports
    with jaydebeapi._jdbc_connect_jpype('org.postgresql.Driver', 'jdbc:postgresql://localhost/sqlalchemyTest', ['postgres', 'postgres'], ["D:\Learn\ApacheBeam\jar\pgjdbc_combined-1.0-SNAPSHOT-jar-with-dependencies.jar"],None) as conn:
    #conn = db_engine.raw_connection()
        from org.apache.arrow.adapter.jdbc import JdbcToArrow
        import org.apache.arrow.memory.RootAllocator as rootallocator
        from org.apache.arrow.adapter.jdbc import JdbcToArrowConfigBuilder
        from org.apache.arrow.adapter.jdbc import JdbcToArrowConfig
        #import java.sql.DriverManager
    #from org.apache.arrow.adapter.memory import RootAllocator
    #dm=java.sql.DriverManager
        #print("typeConn-{}".format(type(conn)))
        #print("sysmaxSize-{}".format(sys.maxsize))
        ra=rootallocator(sys.maxsize)
        configbuild=JdbcToArrowConfigBuilder()
        configbuild.setAllocator(ra)
        configbuild.setTargetBatchSize(5000)
        config=configbuild.build()
        cur=conn.createStatement()
        rs=cur.executeQuery(query)
        batch=JdbcToArrow.sqlToArrowVectorIterator(
        rs,
        config)
        i=0
        while batch.hasNext():
            #combdf=None
            df = jvm.record_batch(batch.next()).to_pandas()
            #print(len(combdf))
            yield df

print(os.environ["PG_JDBC_DRIVER_PATH"])#="D:\Learn\ApacheBeam\jar\postgresql-42.2.16.jar"

con=sqlalchemy.create_engine("jdbcapi+pgjdbc://{}:{}@{}/{}".format('postgres', 'postgres', 'localhost', 'sqlalchemyTest')).execution_options(isolation_level='AUTOCOMMIT')
wcon=sqlalchemy.create_engine("jdbcapi+pgjdbc://{}:{}@{}/{}".format('postgres', 'postgres', 'localhost', 'sqlalchemyTest'))
# @event.listens_for(wcon,"before_cursor_execute")
# def receive_before_cursor_execute(
#        conn, cursor, statement, params, context, executemany
#         ):
#             if executemany:
#                 cursor.fast_executemany = True

print('starttime-{}'.format(datetime.datetime.now()))
#for readChunk in pd.read_sql("select randomText,randomNumber from public.testtable",con,chunksize=100000):
#readChunk=pd.read_sql("select randomText,randomNumber from public.testtable",con,index_col=None,chunksize=None)
#print(readChunk)
    #print("count:{}".format(len(readChunk)))
    #readChunk.to_sql('testtableout1',wcon,schema='public',if_exists='append',index=False)
for readChunk in read_sql_pyarrow("select * from public.testtablelarge",con):
    print("readChunkLen-{}".format(len(readChunk)))
    print('readcomplete-{}'.format(datetime.datetime.now()))
    readChunk.to_sql('testtablelargeout',wcon,schema='public',if_exists='append',index=False,chunksize=5000,method='multi')
print('Endtime-{}'.format(datetime.datetime.now()))