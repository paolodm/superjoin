'''
Created on Oct 14, 2012

@author: adetolaadewodu



'''

from pymongo import Connection
from join import *

def get_columns(database, table_name):
    
    db = connection[database]
    
    columns = db.tables.find_one({"table":table_name})
    
    return columns['columns']

def insert_table(database,table, columns):
    db = connection[database]
    
    tables_collection = db['tables']
    
    table = {"id":table,
             "name":table,
             "table":table,
             "columns": columns}
    
    tables_collection.insert(table)

def insert_slice(database, slice_name):
    db = connection[database]
    
    slices_collection = db['slices']

    slice = {"id":slice_name,
             "name":slice_name,
             "table":slice_name}
    
    slices_collection.insert(slice)

if __name__ == '__main__':
    connection = Connection()
    print "Connection is successful"
    
    database = 'mr_demo'
 
    slice_name = "life_expectancy"

    insert_slice(database, slice_name)
    
    slice_name = "us_economic_assistance"
    
    insert_slice(database, slice_name)
    
    columns = ["id","country", "age"]
    
    insert_table(database, slice_name, columns)
    
    newcolumns = get_columns(database, slice_name)
    
    print newcolumns
    
#    print create_map("country")
    
    print create_map("country", columns)
    
    print "slices inputed"