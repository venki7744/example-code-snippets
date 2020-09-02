import sqlalchemy
import jaydebeapi
#import sqlalchemy-jdbcapi
import pandas as pd
import os
import datetime
from sqlalchemy import event

print(os.environ["PG_JDBC_DRIVER_PATH"])#="D:\Learn\ApacheBeam\jar\postgresql-42.2.16.jar"

con=sqlalchemy.create_engine("jdbcapi+pgjdbc://{}:{}@{}/{}".format('postgres', 'postgres', 'localhost', 'sqlalchemyTest')).execution_options(isolation_level='AUTOCOMMIT')
con=con.raw_connection()
wcon=sqlalchemy.create_engine("jdbcapi+pgjdbc://{}:{}@{}/{}".format('postgres', 'postgres', 'localhost', 'sqlalchemyTest'))
@event.listens_for(wcon,"before_cursor_execute")
def receive_before_cursor_execute(
       conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True

print('starttime-{}'.format(datetime.datetime.now()))
for readChunk in pd.read_sql("select randomText,randomNumber from public.testtable",con,chunksize=200000):
#readChunk=pd.read_sql("select randomText,randomNumber from public.testtable",con,index_col=None,chunksize=None)
#print(readChunk)
    print("count:{}".format(len(readChunk)))
    print('StartLoadChunk-{}'.format(datetime.datetime.now()))
    #readChunk.to_sql('testtableout1',wcon,schema='public',if_exists='append',index=False)
    readChunk.to_sql('testtableout2',wcon,schema='public',if_exists='append',index=False,chunksize=15000,method='multi')
    print('EndLoadChunk-{}'.format(datetime.datetime.now()))
print('Endtime-{}'.format(datetime.datetime.now()))