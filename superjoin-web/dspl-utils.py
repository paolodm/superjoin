'''
Created on Oct 14, 2012

@author: adetolaadewodu
'''
from pymongo import Connection



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
    

    print "slices inputed"