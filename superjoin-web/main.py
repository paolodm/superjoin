__author__ = 'Paolo'

from bottle import route, run, template, get
import pymongo


@get('/metadata')
def metadata():

    db = pymongo.Connection()['mr_demo']

    tables = list(db.metadata.find({}, { 'name': 1}))
    tableNames = [t['name'] for t in tables]

    return {
        'count' : len(tableNames),
        'collections' : tableNames
    }

run(host='localhost', port=8080)