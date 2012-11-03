__author__ = 'Paolo'

from bottle import route, run, template, get, response, static_file
import pymongo
from bson.json_util import dumps

@route('/static/:path#.+#', name='static')
def static(path):
    return static_file(path, root='static')

@route('/main.htm')
def main():
    raise static_file('main.htm', root='')


@get('/slices')
def slices():

    db = pymongo.Connection()['mr_demo']

    tables = list(db.slices.find({}, { 'name': 1}))
    tableNames = [t['name'] for t in tables]

    return {
        'count' : len(tableNames),
        'collections' : tableNames
    }

@route('/tabledata/<name>')
def tabledata(name):
    db = pymongo.Connection()['mr_demo']
    data_collection = db[name]
    
    data = list(data_collection.find({}))
    json_data = dumps(data)

    return { data : json_data }

run(host='localhost', port=8080, reloader=True)