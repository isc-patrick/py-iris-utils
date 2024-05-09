from __future__ import annotations
from pydantic import BaseModel , ConfigDict
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime
import time
from pydantic_settings import BaseSettings

class MyBaseModel(BaseSettings):
   model_config = ConfigDict(extra='allow', populate_by_name=True)

class Parent(MyBaseModel):
    list_name: str

    def get_child(self, name: str):
        lyst = getattr(self, self.list_name)
        for item in lyst:
            if item.name == name:
                return item
        return None
    
    def add(self, child: BaseModel):
        lyst = getattr(self, self.list_name)
        lyst.append(child)
        
    def get_single(self):
        # Often there will be just one
        lyst = getattr(self, self.list_name)
        return lyst[0]
                
class Column(MyBaseModel):

    name: str
    instance_name: str 
    timestamp: Optional[datetime] = None
    database_name: str  
    schema_name: str  
    table_name: str 
    data_type: str  
    character_maximum_length: Optional[int] = None 
    unique_column: Optional[bool] = None
    is_nullable: Optional[bool] = None
    primary_key: Optional[bool] = None
    is_identity: Optional[bool] = None
    index_name: Optional[str] = None
    ordinal_position: Optional[int]  = None
    non_unique: Optional[bool]  = None
    non_unique: Optional[bool]  = None

    def add_data(self, values: dict):
        for k,v in values.items():
            attr = hasattr(self, k)
            if attr:
                setattr(self, k, v)

    # def get_json(self):
    #     col = {'name':self.name, 'unique': self.unique_column, 'primary_key': self.primary_key, 'nullable': self.is_nullable}
    #     return json.dumps(col)
    
class Table(Parent):
    name: str
    field_count: int = 0
    full_name : str = ""
    row_count : int = 0
    schema_name : str = ""
    columns: List[Column] = []
    list_name: str = "columns"
    computations: List[str] = []
   # partition_key: Optional[str] = None

class Schema(Parent):
    name: str
    tables: List[Table] = []
    list_name: str = "tables"
    
class Database(Parent):
    name: str
    schemas: List[Schema] = []
    list_name: str = "schemas"
    
    def get_tables(self):
        tables = []
        for s in self.schemas:
            for t in s.tables:
                tables.append(t)
        return tables

class Instance(Parent):
    name: str
    host: str = ""
    databases: List[Database] = []
    list_name : str ="databases"
    
class Metadata(Parent):
    """_summary_
    Metadata->Instances->Databases->Schemas->Tables->Columns
    """
    timestamp: datetime = time.time()
    instances: List[Instance] = []
    list_name: str ="instances"
    
    def get_db(self):
        return self.get_single().get_single()
    
    def get_tables(self):
        db = self.get_single().get_single()
        return db.get_tables()

class Server(MyBaseModel): 
    name: str
    database: str
    dialect: str
    driver: Optional[str] = None
    host: Optional[str] = ""
    password: Optional[str] = None
    user: Optional[str] = None
    port: Optional[int] = None
    schemas: Optional[list[str]] = []

class LLM(MyBaseModel):
    name: str  

class Configuration(MyBaseModel):
    servers: Optional[List[Server]] = []
    src_server: Optional[str] = None
    llms: Optional[List[LLM]] = None
    active_llm: Optional[str] = None

def get_from_list(lyst: str, name: str) -> BaseModel: 
    for item in lyst:
        if item.name == name:
            return item
    return None