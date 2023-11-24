import get_venv_variable as gav
from create_pyspark import get_spark_object
from validate import get_current_date,print_schema,check_for_nulls
import logging
import logging.config
import os
from ingest import load_files,display_df,df_count
from data_processing import *
from data_transformation import *
from extraction import *
from time import perf_counter
from load import *
from encryption_utils import *
logging.config.fileConfig('Properties/Configuration/logging.config')
from cryptography.fernet import Fernet

def main():
    try: 
        start_time = perf_counter()
        logging.info('I am in the main method...')
        logging.info('Calling  spark object')
        
        spark=get_spark_object(gav.envn,gav.appName)

        logging.info('Validating spark object ')

        get_current_date(spark)

        for file in os.listdir(gav.src_olap):
            print("file is "+ file)

            file_dir=gav.src_olap + '\\' + file
            if file.endswith('.parquet'):
                file_format='parquet'
                header = 'NA'
                inferSchema ='NA'
            elif file.endswith('.csv'):
                file_format='csv'
                header=gav.header
                inferSchema=gav.inferSchema
        logging.info('reading file which is of > {}'.format(file_format))
        df_city = load_files(spark=spark,file_dir=file_dir,file_format=file_format,header=header,
                             inferSchema=inferSchema)
        df_encrypted_city = encrypt_data(df_city,cipher_suite)
        logging.info('displaying file')
        # display_df(df_encrypted_city,'df_city')

        # logging.info("validating the dataframe city...")
        # df_count(df_city,'df_city')

        logging.info("checking for the files in the FACT....")
        for files in os.listdir(gav.src_oltp):
            print("file is "+ files)

            file_dir=gav.src_oltp + '\\' + files
            if files.endswith('.parquet'):
                file_format='parquet'
                header = 'NA'
                inferSchema ='NA'
            elif files.endswith('.csv'):
                file_format='csv'
                header=gav.header
                inferSchema=gav.inferSchema
        logging.info('reading file which is of > {}'.format(file_format))

        df_fact= load_files(spark=spark,file_dir=file_dir,file_format=file_format,header=header,
                            inferSchema=inferSchema)
        # df_fact.show(vertical = True)
        df_encrypte_fact = encrypt_data(df_fact,cipher_suite)
        logging.info('displaying the df_fact dataframe')
        # display_df(df_encrypte_fact,'df_fact')

        # logging.info("validating the dataframe fact...")
        # df_count(df_fact,'df_fact')
# -----------------------------------------
        logging.info("implementing data_processing methods...")
        logging.info("Decrypt Data frame city")
        df_city_sel=decrypt_data(df_encrypted_city,cipher_suite)
        # display_df(df_city_sel,'df_city_sel')

        logging.info("Decrypt Data frame presc")
        df_presc_sel=decrypt_data(df_encrypte_fact,cipher_suite)
        # display_df(df_presc_sel,'df_presc_sel')

        df_city_sel,df_presc_sel=data_clean(df_city_sel,df_presc_sel)

        logging.info("validating  schema for the dataframe...")
        print_schema(df_city_sel,'df_city_sel')
        # display_df(df_city_sel,'df_city_sel')
        print_schema(df_presc_sel,'df_presc_sel')
        # display_df(df_presc_sel,'df_presc_sel')

    #     # # display_df(df_presc_sel,'df_fact')
    #     # logging.info('checking for null values in dataframe ...after processing')
    #     # check_df=check_for_nulls(df_presc_sel,'df_fact')
    #     # # display_df(check_df,'df_fact')
    #     # ------------------------------------------------------------------
        logging.info('data_transformation executing...')

        df_report_1 =data_report1(df_city_sel,df_presc_sel)
        # logging.info('displaying the df_report_1')
        # display_df(df_report_1,'data_report1')
        df_report_1 = encrypt_data(df_report_1,cipher_suite)
        # display_df(df_report_1,'df_report_1')


        # logging.info('displaying data_report2 method....')
        df_report_2 = data_report2(df_presc_sel)
        # display_df(df_report_2, 'data_report2')
        df_report_2 = encrypt_data(df_report_2,cipher_suite)
        # display_df(df_report_2,'df_report_2')
        
    ############################################################################################
        logging.info("extracting files to Output...")
        city_path = gav.city_path
        df_report_1=decrypt_data(df_report_1,cipher_suite)
        extract_files(df_report_1, 'orc', city_path, 1, False, 'snappy')

        presc_path = gav.presc_path
        df_report_2=decrypt_data(df_report_2,cipher_suite)
        extract_files(df_report_2, 'parquet', presc_path, 2, False, 'snappy')

        logging.info("extracting files to output completed.....")

    #     logging.info('writing into hive table')
    #     # data_hive_persist(spark=spark, df=df_report_1, dfname='df_city', partitionBy='state_name', mode='append')
    #     # data_hive_persist(spark=spark, df=df_report_2, dfname='df_presc', partitionBy='presc_state', mode='append')
    #     logging.info("successfully written into Hive")

        logging.info("Now write {} into SQL Server".format(df_report_1))
        load_data_sqlserver(df=df_report_1, dfname='df_city',
        url="jdbc:sqlserver://DESKTOP-H4UBGAI\\SQLEXPRESS:1433;databaseName=Project Data", dbtable='df_city', mode='append',
        user=gav.user, password=gav.password)  

        logging.info("Now write {} into SQL Server".format(df_report_2))
        load_data_sqlserver(df=df_report_2, dfname='df_presc',
        url="jdbc:sqlserver://DESKTOP-H4UBGAI\\SQLEXPRESS:1433;databaseName=Project Data", dbtable='df_presc', mode='append',
        user=gav.user, password=gav.password)

        logging.info("successfully data inserted into table Sql server...")

        end_time = perf_counter()
        print(f"the process time {end_time - start_time: 0.2f} seconds()")

    except Exception as exp:
        logging.error("An error occurred when calling main() please check the trace=== ", str(exp))


if __name__ =='__main__':
    main()
    logging.info('Aplication done')