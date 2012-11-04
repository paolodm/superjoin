__author__ = 'Paolo'

from bottle import route, run, template, get,request, response
import pymongo
from bson.json_util import dumps

@get('/slices')
def getslices():

    db = pymongo.Connection()['mr_demo']

    tables = list(db.slices.find({}, { 'name': 1}))
    tableNames = [t['name'] for t in tables]

    return {
        'count' : len(tableNames),
        'slices' : tableNames
    }

@get('/concepts')
def getconcepts():
    return {'concepts':""}

@get('/tables')
def get_table_info():
    return {'tables':''}

@route('/join')
def join_tables():
    
    table1 = request.query.table1
    table2 = request.query.table2
    joined_column = request.query.column
    tablelist = [table1, table2, joined_column]
    
    # create new table name
    
    # create map
    
    # create reduce
    
    
    
    return dumps(tablelist)
    


@route('/tabledata/<name>')
def gettabledata(name):
    db = pymongo.Connection()['mr_demo']
    data_collection = db[name]
    
    data = list(data_collection.find({}))
    json_data = dumps(data)

    return json_data
    
run(host='localhost', port=8080, reloader=True)