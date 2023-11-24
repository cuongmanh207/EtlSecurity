import os

os.environ['envn']='DEV'
os.environ['header']='True'
os.environ['inferSchema']='True'
os.environ['user'] = 'sa'
os.environ['password'] = 'manhcuong207'

header=os.environ['header']
inferSchema=os.environ['inferSchema']
envn=os.environ['envn']
user = os.environ['user']
password = os.environ['password']

appName='Project pyspark'
current=os.getcwd()

src_olap=current + '\Source\olap'
src_oltp=current + '\Source\oltp'
city_path = 'output\cities'
presc_path = 'output\prescriber'