'''
Created on Oct 14, 2012

@author: adetolaadewodu
'''
from pymongo import Connection

if __name__ == '__main__':
    connection = Connection()
    print "Connection is successful"
    
    db = connection['mr_demo']
    
    slices_collection = db['slices']
    
    slice1 = {"id":"life_expectancy",
             "name":"life expectancy",
             "table":"life_expectancy"}
    
    slice2 = {"id":"us_economic_assistance",
             "name":"us economic assistance",
             "table":"us_economic_assistance"}
    
    slices_collection.insert(slice1)
    slices_collection.insert(slice2)
    print "slices inputed"