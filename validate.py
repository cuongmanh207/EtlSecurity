import logging.config
logging.config.fileConfig('Properties/Configuration/logging.config')
loggers=logging.getLogger('Validate')
from pyspark.sql.functions import *
def get_current_date(spark):
    try:
        loggers.warning('started the get_current_date method...')
        output= spark.sql("""select current_date""")
        loggers.warning("validating spark object with current date-"+ str(output.collect()))
    except Exception as e:
        loggers.error("an error occured in get_current_date",str(e))
        raise
    else:
        loggers.warning('Validation  done , go frwd...')

def print_schema(df,dfName):
    try:
        loggers.warning('print schema method executing....{}'.format(dfName))

        sch=df.schema.fields

        for i in sch:
            loggers.info("\t{}".format(i))
    except Exception as e:
        loggers.error("An error occured at print_schema ::: ",str(e))

        raise
    else:
        loggers.info("print_schema done, go frwd...")
def check_for_nulls(df,dfName):
    try:
        loggers.info('check for nulls method executing ..... for {}'.format(dfName))
        check_null_df=df.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df.columns])
    except Exception as e:
        loggers.error('An error occur while working on check_for_nulls===',str(e))
        raise
    else:
        loggers.warning('check_for_nulls executed successfully....')
    return check_null_df
