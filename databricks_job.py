from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
import yaml
import sys; 
import load 
   
# Created a class object 
object = load.Load

def load_func(dotpath : str):
      """ load function in module.  function is right-most segment """
      module_, func = dotpath.rsplit(".", maxsplit=1)
    #module = importlib.import_module(module_)
    #m = import_module(module_)
    #return getattr(m, func)
      return func




spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()


file_path = "C:\Users\636371\Downloads\json\*.json"
with open('C:\\Users\\636371\\lambda\\sam-test\\VA_Framework\\resources\\config.yaml', 'r') as file:
            meta_data = yaml.safe_load(file)
            print(meta_data)
            pr = meta_data['validatons']
            print(pr)
            #field1 = pr['args1'].strip()

           # func = load_func(pr['function'])
# Read JSON file into dataframe    
            df1 = spark.read.json(file_path)
            data = meta_data['validatons']
            for i in data:
              print(i)
              field1 = (list(i.values())[2]).strip()
              print((list(i.values())[1]))
              df1 = object.call(list(i.values())[1],field=field1,df=df1)
              df1.show()
              
                    
                     
            
           # df2 = object.call(pr['function'],field=field1,df=df1)
           # df2.show()
           # df1.printSchema()
           # df1.show()
