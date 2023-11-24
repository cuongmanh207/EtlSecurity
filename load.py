import logging.config

from pyspark.sql.functions import *
from pyspark.sql.types import *

from datetime import datetime as date

logging.config.fileConfig('Properties/configuration/logging.config')

loggers = logging.getLogger('Load')
from prefect import flow, task

# def data_hive_persist(spark, df, dfname, partitionBy, mode):
#     try:
#         loggers.warning('persisting the data into Hive Table for {}'.format(dfname))

#         loggers.warning('lets create a database....')

#         spark.sql(""" create database if not exists cities """)

#         spark.sql("""use cities """)

#         loggers.warning("No writing {} into hive_table by {} ".format(df, partitionBy))

#         df.write.saveAsTable(dfname, partitionBy=partitionBy, mode=mode)


#     except Exception as exp:
#         loggers.error("An error occurred while processing data_hive_persist::::", str(exp))

#         raise
#     else:
#         loggers.warning('Data successfully persisted into hive tables...')

@task
def load_data_sqlserver(df, dfname, url,  dbtable, mode, user, password):
    try:
        loggers.warning('executing the load_data_sqlserver method...{}'.format(dfname))
        df.write \
            .format("jdbc") \
            .mode(mode) \
            .option("url", url) \
            .option("dbtable", dbtable) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .save()
    except Exception as e:
        loggers.error("An error occured @ load_data_sqlserver method::::", str(e))

        raise

    else:
        loggers.warning('load_data_sqlserver method executed succesfully...into {}'.format(dbtable))