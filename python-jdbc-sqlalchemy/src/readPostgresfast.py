import jaydebeapi
import pandas as pd
import os
import datetime
from pyarrow import jvm
import jpype
import sys
import math
import jaydebeapi

def fix_values(values):
    outvalues=[]
    for value in values:
        if value is None:
            value='NULL'

        if isinstance(value,(int,float)):
            if math.isnan(value):
                value='NULL'
            value=str(value)
        else:
            value="'{}'".format(str(value).replace("'","''"))
        outvalues.append(value)
    return outvalues


def read_sql_pyarrow(query,conn,batchsize=100000):
    from jpype import imports
    with jaydebeapi._jdbc_connect_jpype(**srccon) as conn:
        #conn = db_engine.raw_connection()
        from org.apache.arrow.adapter.jdbc import JdbcToArrow
        import org.apache.arrow.memory.RootAllocator as rootallocator
        from org.apache.arrow.adapter.jdbc import JdbcToArrowConfigBuilder
        from org.apache.arrow.adapter.jdbc import JdbcToArrowConfig
        from org.apache.arrow.adapter.jdbc import JdbcToArrowUtils

        ra=rootallocator(sys.maxsize)
        ca=JdbcToArrowUtils.getUtcCalendar()
        configbuild=JdbcToArrowConfigBuilder(ra,ca,True)
        configbuild.setTargetBatchSize(batchsize)
        config=configbuild.build()
        cur=conn.createStatement()
        rs=cur.executeQuery(query)
        batch=JdbcToArrow.sqlToArrowVectorIterator(
        rs,
        config)

        while batch.hasNext():
            df = jvm.record_batch(batch.next()).to_pandas()
            yield df

def insert_sql_build(values:list,destschema,desttable,columns):
    finalQuery=str("insert into {}.{}({}) values {}".format(destschema,desttable,columns,','.join(values)))
    return finalQuery

print(os.environ["PG_JDBC_DRIVER_PATH"])#="D:\Learn\ApacheBeam\jar\postgresql-42.2.16.jar"

srccon={"jclassname":'org.postgresql.Driver', 
"url":'jdbc:postgresql://localhost/sqlalchemyTest', 
"driver_args":['postgres', 'postgres'], 
"jars":["D:\Learn\ApacheBeam\jar\pgjdbc_combined-1.0-SNAPSHOT-jar-with-dependencies.jar"]
,"libs":None}

srcquery="select * from public.testtablelarge limit 1000000"

destcon={"jclassname":'org.postgresql.Driver', 
"url":'jdbc:postgresql://localhost/sqlalchemyTest', 
"driver_args":['postgres', 'postgres'], 
"jars":["D:\Learn\ApacheBeam\jar\pgjdbc_combined-1.0-SNAPSHOT-jar-with-dependencies.jar"]
,"libs":None}

desttable="testtablelargeo"
destschema="public"

with jaydebeapi.connect(**destcon) as wcon:
    wcon.jconn.setAutoCommit(False)
    print('starttime-{}'.format(datetime.datetime.now()))
    try:
        for readChunk in read_sql_pyarrow(srcquery,srccon):
            print('readcomplete-{}'.format(datetime.datetime.now()))
            columns=','.join(readChunk.columns)
            values=[]
            chunksize=20000
            i=0
            wcur=wcon.cursor()
            for row in readChunk.itertuples(index=False):
                outrow=','.join(fix_values(row))
                values.append(format("({})".format(outrow)))
                i=i+1
                if i%5000==0:
                    print("1000chunkready-{}".format(datetime.datetime.now()))
                if i==chunksize:
                    print("chunkready-{}".format(datetime.datetime.now()))
                    #finalQuery=str("insert into public.testtablelargeo({}) values {}".format(columns,','.join(values)))
                    wcur.execute(insert_sql_build(values,destschema,desttable,columns))
                    values=[]
                    i=0
                    print("chunkLoaded-{}".format(datetime.datetime.now()))
            if values !=[]:
                print("Lastchunkready-{}".format(datetime.datetime.now()))
                #finalQuery=str("insert into public.testtablelargeo({}) values {}".format(columns,','.join(values)))
                wcur.execute(insert_sql_build(values,destschema,desttable,columns))
                print("LastchunkLoaded-{}".format(datetime.datetime.now()))
    except jaydebeapi.Error as e:
        wcon.rollback()
    finally:
        wcon.commit()
print('Endtime-{}'.format(datetime.datetime.now()))