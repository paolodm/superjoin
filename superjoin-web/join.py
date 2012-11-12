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
    
    for column in columns:
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
  
    code_text = """
    function(key, values) {
    
        var merged = {};
    
        values.forEach(function(value) {
            for (var attrname in value) {
    
                if (!merged.hasOwnProperty(attrname))
                    merged[attrname] = value[attrname];
            }
        });
    

    
        return merged;
    }
    """ 
#//% joined_column
    
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
    slices_collection = db['slices']
        
    slice1 = {"id":"joined",
                 "name":"joined",
                 "table":"joined"}
    
    slices_collection.insert(slice1)
     
    life_expect_map = create_map("country", ['age'])

    life_expect_reducer = create_reducer("country")
    
    stunted_map = create_map("country", ["Year", "Value"])
   
    stunted_reducer = create_reducer("country")
    
    print stunted_map
    print stunted_reducer
    
    print life_expect_map
    print life_expect_reducer
    
    slice1 = {"id":"life_stunted",
                 "name":"life_stunted",
                 "table":"life_stunted"}
    
    slices_collection.insert(slice1)
    
    db.stunted_age.map_reduce(stunted_map,stunted_reducer,{'reduce':'life_stunted'} )
    db.life_expectancy.map_reduce(life_expect_map, life_expect_reducer, {'reduce': 'life_stunted'} )

    
    #print db.joined.find({'value.dollars': {'$gt':0}, 'value.life_expectancy': {'$gt':0}})[2]