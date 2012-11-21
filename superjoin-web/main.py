__author__ = 'Paolo'


from bottle import route, run, template, get,request, response,static_file, post
import pymongo
import MongoEncoder
from join import *
from bson import BSON  
from bson.json_util import dumps
from dspl_utils import *

@route('/static/:path#.+#', name='static')
def static(path):
    return static_file(path, root='static')

@route('/main.htm')
def main():
    raise static_file('main.htm', root='')


@get('/slices')
def getslices():

    db = pymongo.Connection()['mr_demo']

    tables = list(db.slices.find({}))
    tableNames = [t['name'] for t in tables]

    return {
        'count' : len(tables),
        'slices' : tableNames
    }


@get('/join')
def join():
 
    joined_column = request.query.joined_column
    print "The joined colum is %s" % joined_column
    
    table1 = request.query.slice1
    table2 = request.query.slice2
  
    database = 'mr_demo'
  
    # get table1 columns
    columns1 = get_columns(database, table1)
    
    # get table1 columns
    columns2 = get_columns(database, table2)
#    
    joined_list = create_inner_join_table(table1, table2, joined_column, columns1, columns2)
    
    json_data = dumps(joined_list)
    return {"table": json_data}
    
    


@route('/tabledata/<name>')
def gettabledata(name):
    db = pymongo.Connection()['mr_demo']
    data_collection = db[name]
    
    data = list(data_collection.find({}))
    json_data = dumps(data)

    return { "data" : json_data }

run(host='localhost', port=8080, reloader=True)