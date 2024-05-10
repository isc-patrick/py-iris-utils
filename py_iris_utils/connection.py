import jaydebeapi
from loguru import logger
from sqlalchemy import create_engine,  MetaData, Engine
import sqlite3
import pandas as pd
import json 
from dotenv import load_dotenv

from py_iris_utils.models import Metadata, Instance, Database, Schema, Table, Column
from py_iris_utils.models import Configuration, Server, get_from_list

load_dotenv()

# TODO load from config
LOGGING = True

class IRISConnection():

    def __init__(self, config: Configuration):
        self.engine = get_alchemy_engine(config)

    def load_metadata(self):
        self.metadata = get_alchemy_engine(self.config, engine=self.engine)

###############################################################################
# METADATA FUNCTIONS
###############################################################################
def get_metadata(config: Configuration = None, server: Server = None):
    """_summary_
    Uses the src_server from the config to deteminre datasource and loads metadata for all schemas specified.
    Args:
        ctx (Context): Context object

    Returns:
        insights.model.Metadata: Contains a heirarchy of instance->database->schema->table->column
    """
    
    if config:
        server = get_from_list(config.servers, config.src_server)
    
    alchemy_metadata = get_metadata_alchemy(server=server)
    metadata = load_metadata_to_models(alchemy_metadata, server)

    return metadata, alchemy_metadata

def get_metadata_alchemy(config: Configuration = None, server: Server = None, engine: Engine = None):
    
    if config:
        server = get_from_list(config.servers, config.src_server)
    
    if not server:
        raise Exception("Context, Config or Server nust be provided to get active db server")
    
    if not engine:
        connection_url = create_connection_url(server)
        engine = create_engine(connection_url)
    
    print(f"Connecting to engine {engine.url} : {engine.connect()}")
    metadata_obj = MetaData()
    if server.schemas:
        for schema in server.schemas:
            try:
                metadata_obj.reflect(engine, schema)
            except Exception as ex:
                logger.error(f"Error getting metadata for {schema}: {ex}")
    else:
        metadata_obj.reflect(engine)

    return metadata_obj

def load_metadata_to_models(alchemy_metadata: MetaData, server: Server):
    
    metadata = Metadata()
    instance = Instance(name=server.name, host=server.host)
    metadata.instances.append(instance)
    
    database = Database(name=server.database)
    instance.databases.append(database)
    
    if server.schemas:
        for schema_name in server.schemas:
            schema = Schema(name=schema_name)
            database.schemas.append(schema)
    else:
        schema = Schema(name="default")
        database.schemas.append(schema)
    
    for src_table_name, src_table in alchemy_metadata.tables.items():
        # Add the table to the schema
        #logger.debug(f"loading table {src_table.fullname}")
        table_schema = src_table.schema if src_table.schema else "default"
        table = Table(name=src_table_name, full_name=src_table.fullname)
        schema = get_from_list(database.schemas, table_schema)
        
        # Only schemas listed in config will exist already
        if schema:
            schema.tables.append(table)
            
            table_field_count = 0
            # Add the columns to the table
            for col_name, col in src_table.columns.items():
                logger.debug(f"{col_name}, {col.type}, {type(col.type)}, {str(col.type)}, {col.index}, {col.unique}, {col.primary_key}, {col.nullable}")
                
                column = Column(name=col_name, instance_name=instance.name, database_name=database.name, 
                                schema_name=table_schema, table_name=src_table_name, data_type=str(col.type),
                                is_nullable=col.nullable, primary_key=col.primary_key, unique_column=col.unique)
                
                table.columns.append(column)
                table_field_count += 1
                
            setattr(table, "field_count", table_field_count)
        
    return metadata

################################################################################
# SQL CONNECTION FUNCTIONs
################################################################################

def get_alchemy_engine(config: Configuration, server: Server = None):  
    
    if not server:
        server = get_from_list(config.servers, config.src_server)
    
    connection_url = create_connection_url(server)
    engine = create_engine(connection_url)
 
    return engine

def create_connection_url(server: Server):
     # Create a connection url from the server properties in this form dialect+driver://username:password@host:port/database
     # Only adding sections if they have a value in the server instance
     
     # sqlite requires 3 slashes for reference to file db
     seperator = ":///" if server.dialect == "sqlite" else "://"
     driver_dialect = f"{server.dialect}{seperator}" #if not server.driver else f"{server.dialect}+{server.driver}{seperator}"
     user_pass = f"{server.user}:{server.password}@" if server.user and server.password else ""
     host_port = f"{server.host}:{server.port}/" if server.host and server.port else ""
     database = f"{server.database}"
     
     return driver_dialect+user_pass+host_port+database

def get_connection(config, conn_type, instance = ""):
    server_name = instance if instance else config.src_server
    if conn_type == "jdbc":
        return get_jdbc_connection(config, server_name)
    elif conn_type == "sqlite":
        return get_sqlite_connection(config, server_name)
    else:
        logger.warning(f"Connection type {conn_type} not supported")
        raise Exception(f"Connection type {conn_type} not supported")

def get_jdbc_connection(config: Configuration, server_name: str):
    server = get_from_list(config.servers, server_name)
    logger.debug(f"Server name {server_name}, {server}")
    db = f"{server.host}:{server.port}/{server.database}"
    conn_url = f"jdbc:IRIS://{db}"
    print(f"Connection URL {conn_url}")
    conn = jaydebeapi.connect(server.driver,
                            conn_url,
                            [server.user, server.password],
                            config.spark_jars)

    return conn

def get_sqlite_connection(config, server_name):

     server = get_from_list(config.servers, server_name)
     print(f"Connecting to DB {server.database}")
     conn = sqlite3.connect(server.database)
     return conn

################################################################################
# SQL HELPER FUNCTIONs
################################################################################

def convert_to_csv(headers, rows):
    """Converts a list of rows to a CSV string.

    Args:
        col: A list of rows, where each row is a list of values.
        rows: A list of rows, where each row is a list of values.

    Returns:
        A CSV string.
    """
    output = io.StringIO()
    writer = csv.writer(output)
    headers = headers if isinstance(headers, list) else [headers]
    rows = rows if isinstance(rows, list) else [rows]
    writer.writerow(headers)
    writer.writerows(rows)
    return output.getvalue()

def execute_sql_scalar(engine, response: dict):
    """
    Apply a SQL query to the database and return the single row result
       For example, This can be used for All queries that start with SELECT COUNT(*) 
    """
    results = execute_sql(engine, response, return_df=False)
    return results 

def execute_sql(engine, response: dict, return_df: bool = True):
    """
    Apply a SQL query to the database
    """
    try: 
        conn = engine.connect()
        sql = response.get("sql")
        if not sql:
            logger.error(f"No SQL provided but action execute_sql returned")
            return None 
        sql = sql.replace(";","")
        df = pd.read_sql(sql, conn)
        #df.reset_index(drop=True, inplace=True)
        if return_df:
            return df
        
        results = io.StringIO()
        df.to_csv(results, index=False)
        return results.getvalue()
    except Exception as ex:
        logger.error(f"Error executing SQL - {sql} - {ex}")
        return None

################################################################################
# MISC
################################################################################
def log(msg):

    delimiter = "~"
    if LOGGING:
        lines = msg.split("\n")
        for line in lines:
            logger.debug(delimiter+line)
        logger.debug(delimiter)

def load_config(path):

    with open(path) as fo:
        config_dict = json.load(fo)
    
    config = Configuration(**config_dict)
    return config
