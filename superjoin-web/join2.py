__author__ = 'Paolo'
import pymongo
from bson.code import Code
from itertools import izip

db = pymongo.Connection()['mr_demo']

def create_map(joined_column, columns):
    
    column_dict = dict()
    
    for column in columns:
        if not (joined_column == column):
            column_dict[column] = """this.%s""" % column

    print ','.join(column_dict)

    return Code(
        """
        function () {
            emit(this.%s, %s);
        }
        """ % (joined_column, str(column_dict).replace("'", ""))
    )

def create_reducer(joined_column):
    return Code(
    """
    function(key, values) {
    
        var merged = {};
    
        values.forEach(function(value) {
            for (var attrname in value) {
                if (!merged.hasOwnProperty(attrname))
                    merged[attrname] = value[attrname];
            }
        });
    
        merged['%s'] = key;
    
        return merged;
    }
    """ % joined_column)


def join(table1, table2, join_column):
    slices_collection = db['slices']

    transform(table1, join_column, ['lifespan'])
    transform(table2, join_column, ['age'])

def transform(table, join_column, columns):
    life_expect_map = create_map(join_column, columns)
    life_expect_reducer = create_reducer(join_column)

    print "Map: %s" % life_expect_map

    print "Reducer: %s" % life_expect_reducer

    table.map_reduce(life_expect_map, life_expect_reducer, {'reduce': 'joined'} )

if __name__ == '__main__':
    join(db.life_expectancy, db.life_stunted, 'country')
