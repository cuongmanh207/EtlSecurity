import os
os.environ["PYSPARK_PYTHON"] = "C:/Users/PC/AppData/Local/Programs/Python/Python310/python.exe"
from pyspark.sql import SparkSession

# Tạo một phiên Spark

spark = SparkSession.builder \
    .appName("PySpark Example") \
    .getOrCreate()
# Tạo một DataFrame từ một danh sách
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Hiển thị dữ liệu
df.show()

# Thực hiện các phép biến đổi trên DataFrame
filtered_df = df.filter(df.Age > 30)
result = filtered_df.collect()

# In kết quả
for row in result:
    print(row)

# # Đóng phiên Spark
# spark.stop()
