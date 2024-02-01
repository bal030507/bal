import pyspark
from pyspark.sql import SparkSession
from SparkJob import SparkJob
import yaml
import load 
   
# Created a class object 
object = load.Load
class CCP(SparkJob):
    def __init__(self, name, yaml_file): 
  
        # this is how we call super 
        # class's constructor 
        super().__init__(name) 
        self.name = name
        self.yaml_file = yaml_file
        #self.read 
    def load_all_data(self):
         spark = SparkSession.builder \
        .master("local[1]") \
        .appName("SparkByExamples.com") \
        .getOrCreate()
         file_path = "C:\\Users\\636371\\Downloads\\mrn.json"
         self.df = spark.read.json(file_path)
         #file_path = "C:\Users\636371\Downloads\json\*.json"
         with open(self.yaml_file, 'r') as file:
            self.meta_data = yaml.safe_load(file)
            #print(meta_data)
            #pr = meta_data['validatons']
            #print(pr)
            #field1 = pr['args1'].strip()

           # func = load_func(pr['function'])
# Read JSON file into dataframe    
           # df1 = spark.read.json(file_path)
            #data = meta_data['validatons']
            #for i in data:
             # print(i)
             # field1 = (list(i.values())[2]).strip()
             # print((list(i.values())[1]))
             # df1 = object.call(list(i.values())[1],field=field1,df=df1)
             # df1.show()
    
    def run(self):
        #print(self.name)
        #print(self.file)
        spark = SparkSession.builder \
        .master("local[1]") \
        .appName("SparkByExamples.com") \
        .getOrCreate()
        self.load_all_data()
        data = self.meta_data['validatons']
        for i in data:
              print(i)
              field1 = (list(i.values())[2]).strip()
              print((list(i.values())[1]))
              df1 = object.call(list(i.values())[1],field=field1,df=self.df)
              df1.show()
        self.df.show()


def main():
    x = "bal"
    a = CCP("name", "C:\\Users\\636371\\lambda\\sam-test\\VA_Framework\\resources\\config.yaml") 
    a.run()

if __name__=="__main__": 
    main()