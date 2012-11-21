'''
Created on Oct 14, 2012

@author: adetolaadewodu



'''

from pymongo import Connection
from join import *

def get_columns(database, table_name):
    
    db = Connection()[database]
    
    columns = db.tables.find_one({"table":table_name})
    column_list = ()
    
    for column in columns['columns']:
        column_list = column_list + (column.encode('ascii','ignore'),)
    
    return column_list

def get_columns_without_joined(database, table_name,joined_column):
    
    db = Connection()[database]
    
    columns = db.tables.find_one({"table":table_name})
    column_list = ()
    
    for column in columns['columns']:
        if(column.encode('ascii','ignore') != joined_column):
            column_list = column_list + (column.encode('ascii','ignore'),)
    
    return column_list


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
    columns = ["id","country", "age"]
    
    insert_slice(database, slice_name)
    insert_table(database, slice_name, columns)
    
    slice_name = "stunted_age"
    
    columns = ["country","year","value"]
    insert_slice(database, slice_name)
    insert_table(database, slice_name, columns)

    newcolumns = get_columns_without_joined(database, slice_name, "country")
    
    print newcolumns
    
    print "slices inputed"