__author__ = 'Paolo'
import pymongo
from bson.code import Code
from itertools import izip
from dspl_utils import *


db = pymongo.Connection()['mr_demo']

life_expect_map  = Code("""
    function () {
   emit(this.country, {life_expectancy: this.lifespan });
    }
""")

us_econ_map   = Code("""
    function () {
  // The data set contains grant amounts going back to 1946.  I
  // am only interested in 2009 grants.
  if (this.FY2009 !== undefined && this.FY2009 !== null) {
    emit(this.country_name, {
      dollars: this.FY2009
    });
  }
}
""")


def create_map(joined_column, columns):
    
    column_dict = dict()
    
    for column in tuple(columns):
        if not (joined_column == column):
            column_dict[column] = """this.%s""" % column
    
    
    print ','.join(column_dict)
    
    code_text = """
    function () {
   emit(this.%s, %s);
    }
    """ % (joined_column, str(column_dict).replace("'", ""))
    
    map_code = Code(str(code_text))
    
    return map_code

def create_reducer(joined_column):
 
#                 if (!merged.hasOwnProperty(attrname)) 
    code_text = """
    function(key, values) {
    
        var merged = {};
    
        values.forEach(function(value) {
            for (var attrname in value) {
    
                    
                    merged[attrname] = value[attrname];
            }
        });
    
        merged['%s'] = key;
    
        return merged;
    }
    """ % joined_column
    
    reduce_code = Code(code_text)
    
    return reduce_code

def create_outer_join_table(table1, table2, join_column, column_list_1, column_list_2):
    temp_table = 'temp'
    db[temp_table].remove()
    
    slices_collection = db['slices']
    
    join_map_1 = create_map(join_column, column_list_1)

    join_reducer_1 = create_reducer(join_column)

    join_map_2 = create_map(join_column, column_list_2)
   
    join_reducer_2 = create_reducer(join_column)
    
    join_table_name = "%s_%s" % (table1, table2)
    slice1 = {"id":join_table_name,
                 "name":join_table_name,
                 "table":join_table_name}
    
    db[join_table_name].remove()
    
    slices_collection.insert(slice1)

    db[table2].map_reduce(join_map_2,join_reducer_2,{'reduce':temp_table} )
    
    db[table1].map_reduce(join_map_1, join_reducer_1, {'reduce': temp_table} )

    # Add the values from temp table into new list
    values = []
    for row in list(db[temp_table].find()):
        if row['value'].has_key(join_column): 
            values.append(row['value'])
        else:
            temp_dict = row['value']
            temp_dict[join_column] = row['_id']
            values.append(temp_dict)
    # Insert list into new joined collection
    db[join_table_name].insert(values)
    
    
def create_inner_join_table(table1, table2, join_column, column_list_1, column_list_2):
    temp_table = 'temp'
    db[temp_table].remove()
    
    slices_collection = db['slices']
    
    join_map_1 = create_map(join_column, column_list_1)

    join_reducer_1 = create_reducer(join_column)

    join_map_2 = create_map(join_column, column_list_2)
   
    join_reducer_2 = create_reducer(join_column)
    
    join_table_name = "%s_%s" % (table1, table2)
    slice1 = {"id":join_table_name,
                 "name":join_table_name,
                 "table":join_table_name}
    
    db[join_table_name].remove()
    
    slices_collection.insert(slice1)

    db[table2].map_reduce(join_map_2,join_reducer_2,{'reduce':temp_table} )
    
    db[table1].map_reduce(join_map_1, join_reducer_1, {'reduce': temp_table} )

    # Add the values from temp table into new list
    values = []
    for row in list(db[temp_table].find()):
        if row['value'].has_key(join_column): 
            values.append(row['value'])
            
    # Insert list into new joined collection
    db[join_table_name].insert(values)
    print db[join_table_name].find({})
    return values

reducer = Code("""
function(key, values) {

	var merged = {};

	values.forEach(function(value) {
    	for (var attrname in value) {

    	    if (!merged.hasOwnProperty(attrname))
                merged[attrname] = value[attrname];
    	}
    });

    merged['country'] = key;
    merged['creator'] = "paolo";

	return merged;
}
""")



if __name__ == '__main__':
    
    table1 = 'stunted_age'
    table2 = 'life_expectancy'
    
    join_column = "country"

    database = 'mr_demo'
        
        # get table1 columns
#    columns1 = get_columns_without_joined(database, table1, join_column)
    columns1 = ['year', 'value']
    
    columns2 = ['age']
    # get table1 columns
#    columns2 = get_columns_without_joined(database, table2, join_column)
    
    print columns1
    
    print columns2
    
    join_list = create_inner_join_table(table2, table1, join_column, columns2, columns1)
#    
    print join_list
    