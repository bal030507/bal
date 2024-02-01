import yaml
import importlib
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
import operator

class Load:
    




     def verify_null(field, df:DataFrame):
        return df.filter(col(field) == "sunrise")
     def verify_file_format(field, df:DataFrame):
        return df.filter(col(field) == "OH")

     function_mappings = {
        'verify_null': verify_null,
        'verify_file_format': verify_file_format,
         "add": operator.add,
    }

     def call(func_name, *args, **kwargs):
          if(Load.function_mappings[func_name]):
            return Load.function_mappings[func_name](*args, **kwargs)
          else:
             pass
    



     def load_func(dotpath : str):
      """ load function in module.  function is right-most segment """
      module_, func = dotpath.rsplit(".", maxsplit=1)
    #module = importlib.import_module(module_)
    #m = import_module(module_)
    #return getattr(m, func)
      return func

     def load(**kwargs):
        pr = ""
        with open('C:\\Users\\636371\\lambda\\sam-test\\VA_Framework\\resources\\config.yaml', 'r') as file:
            meta_data = yaml.safe_load(file)
            pr = meta_data['validation1']
            print(pr)
            func = Load.load_func(pr['function'])
            Load.call(func,df=DataFrame, col=pr['args1'])
     
     
