
__author__ = 'Paolo'
import pymongo
from bson.code import Code

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

def create_map(joined_column):
    
    map_code = Code("""
    function(){
    emit(this.country, 
    
    """)
    
    return map_code
    
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

slices_collection = db['slices']
    
slice1 = {"id":"joined",
             "name":"joined",
             "table":"joined"}

slices_collection.insert(slice1)

db.life_expectancy.map_reduce(life_expect_map, reducer, {'reduce': 'joined'} )
db.us_economic_assistance.map_reduce(us_econ_map, reducer, {'reduce': 'joined'} )
#print db.joined.find({'value.dollars': {'$gt':0}, 'value.life_expectancy': {'$gt':0}})[2]