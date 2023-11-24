import logging.config
logging.config.fileConfig('Properties/Configuration/logging.config')
loggers=logging.getLogger("Data_processing")
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prefect import flow, task
@task
def data_clean(df1,df2):
    try:
        loggers.warning('data_clean method() start...')
        
        loggers.warning("selecting required columns and converting some of columns into upper case...")

        df_city_sel= df1.select(upper(col('city')).alias('city'),df1.state_id,upper(df1.state_name).alias('state_name'),
                                upper(df1.county_name).alias('county_name'),df1.population,df1.zips)
        
        loggers.warning("working on OLTP dataset and selecting couple of columns and rename,...")

        df_presc_sel=df2.select(df2.npi.alias('presc_id'),df2.nppes_provider_last_org_name.alias('presc_lname'),
                                df2.nppes_provider_first_name.alias('presc_fname'),df2.nppes_provider_city.alias('presc_city'),
                                df2.nppes_provider_state.alias('presc_state'),df2.specialty_description.alias('presc_spclt'),
                                df2.drug_name,df2.total_claim_count.alias('tx_cnt'),df2.total_day_supply,df2.total_drug_cost,
                                df2.years_of_exp)
        loggers.warning('Adding a new column to df_presc_sel')
        df_presc_sel=df_presc_sel.withColumn('Country_name',lit('USA'))

        loggers.warning('converting years_of_exp string to int and replacing =')

        df_presc_sel=df_presc_sel.withColumn('years_of_exp',regexp_replace(col('years_of_exp'),r"^="," "))
        df_presc_sel=df_presc_sel.withColumn('years_of_exp',col('years_of_exp').cast('int'))

        loggers.warning('concat first and lname')
        df_presc_sel=df_presc_sel.withColumn('presc_fullname', concat_ws(" ",'presc_lname','presc_fname'))

        loggers.warning('Now droping presc_lname,presc_fname')
        df_presc_sel=df_presc_sel.drop('presc_fname','presc_lname')

        loggers.warning("now check for null values in all columns")

        # df_presc_sel=df_presc_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_presc_sel.columns])

        loggers.warning("drop the null values in the respective columns...")

        df_presc_sel=df_presc_sel.dropna(subset='presc_id')
        df_presc_sel=df_presc_sel.dropna(subset='drug_name')

        loggers.warning('fill the null values in tx_cnt with the avg values...')
        mean_tx_cnt=df_presc_sel.select(mean(col('tx_cnt'))).collect()[0][0]
        if mean_tx_cnt is not None and isinstance(mean_tx_cnt, (float, int)):
            df_presc_sel = df_presc_sel.fillna(mean_tx_cnt, 'tx_cnt')
        else:
            # Xử lý trường hợp mean_tx_cnt không hợp lệ
            # Ví dụ: đặt một giá trị mặc định hoặc xử lý lỗi
            pass
        # df_presc_sel=df_presc_sel.select([count(when(isnan(c) | col(c).isNull(),c)).alias(c) for c in df_presc_sel.columns])

        loggers.warning("successfully droped the null values...")

    except Exception as exp:
        loggers.error(f"An error occurred at data_clean() method: {exp}")
        raise
    else:
        loggers.warning("data cleaing method executed done, go frwd...")
    return df_city_sel,df_presc_sel
