from abc import ABC, abstractmethod

class SparkJob(ABC):
    
    def __init__(self, name):
        self.name = name
        print("hello world")
        
    @abstractmethod
    def run(self):
        pass
    
