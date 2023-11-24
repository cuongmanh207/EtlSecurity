from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet
from pyspark.sql.functions import *
key = Fernet.generate_key()
cipher_suite = Fernet(key)

# def encrypt_value(value):
#     if value is not None:
#         try:
#             return cipher_suite.encrypt(value.encode()).decode()
#         except Exception as e:
#             return None
#     return None
# encrypt_udf = udf(encrypt_value, StringType())
# def encrypt_data(df,cipher_suite):
#     for column in df.columns:
#         df = df.withColumn(column, encrypt_udf(df[column]))
#     return df
def encrypt_value(value):
    value_str = '' if value is None else str(value)
    try:
        encrypted_value = cipher_suite.encrypt(value_str.encode()).decode()
        return encrypted_value
    except Exception as e:
        return None  

encrypt_udf = udf(encrypt_value, StringType())

def encrypt_data(df, cipher_suite):
    for column in df.columns:
        df = df.withColumn(column, encrypt_udf(col(column)))
    return df

def decrypt_value(value):
    if value is not None:
        try:
            return cipher_suite.decrypt(value.encode()).decode()
        except Exception as e:
            return None
    return None

decrypt_udf = udf(decrypt_value, StringType())

def decrypt_data(df,cipher_suite):
    for column in df.columns:
        df = df.withColumn(column, decrypt_udf(df[column]))
    return df
