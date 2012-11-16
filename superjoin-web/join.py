__author__ = 'Paolo'
import pymongo
from bson.code import Code
from itertools import izip



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
    
    temp_table = 'temp'
    db[temp_table].remove()
    
    slices_collection = db['slices']
    
    join_column = "country"
    life_expect_map = create_map(join_column, ('country','age'))

    life_expect_reducer = create_reducer(join_column)
    
    stunted_map = create_map(join_column, ("year", "value"))
   
    stunted_reducer = create_reducer(join_column)
    
    print stunted_map
    print stunted_reducer
    
    print life_expect_map
    print life_expect_reducer
    
    join_table_name = "life_stunted"
    slice1 = {"id":join_table_name,
                 "name":join_table_name,
                 "table":join_table_name}
    
    db[join_table_name].remove()
    
    slices_collection.insert(slice1)

    db.stunted_age.map_reduce(stunted_map,stunted_reducer,{'reduce':temp_table} )
    db.life_expectancy.map_reduce(life_expect_map, life_expect_reducer, {'reduce': temp_table} )

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
    db['life_stunted'].insert(values)
    