__author__ = 'Paolo'

from bottle import route, run, template, get
import pymongo


@get('/slices')
def metadata():

    db = pymongo.Connection()['mr_demo']

    tables = list(db.slices.find({}, { 'name': 1}))
    tableNames = [t['name'] for t in tables]

    return {
        'count' : len(tableNames),
        'collections' : tableNames
    }

run(host='localhost', port=8080)