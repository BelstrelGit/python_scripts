# import the module
from __future__ import print_function
import aerospike
from aerospike  import predicates as p
import logging


# Configure the client
config = {
  'hosts': [ ('127.0.0.1', 3000) ]
}

# Create a client and connect it to the cluster
try:
  client = aerospike.client(config).connect()
except:
  import sys
  print("failed to connect to the cluster with", config['hosts'])
  sys.exit(1)

# Records are addressable via a tuple of (namespace, set, key)
#key = ('test', 'demo', 'foo')

# Write a record 
def add_customer(customer_id, phone_number, lifetime_value):
  key = ('test', 'demo', customer_id)
  try:
    client.put(key, {
    'phone': phone_number, 
    'ltv': lifetime_value})
  except Exception as e:
   import sys
   print("error: {0}".format(e), file=sys.stderr)

#  get_ltv_by_id  return ltv
def get_ltv_by_id(customer_id):
    key = ('test', 'demo', customer_id)
    (key, metadata, record) = client.get(key)
    if(record == {}):
     logging.error('Requested non-existent customer ' + str(customer_id))
    else:
     return record.get('ltv')



# Next methods with queries require a secondary index to exist on the bin being queried
# if you have  secondary index comment this string ->
     #client.index_integer_create('test', 'demo', 'phone', 'phone')
client.index_integer_create('test', 'demo', 'phone', 'phone')

#def get_ltv_by_phone return ltvs array   
def get_ltv_by_phone(phone_number):
  
  query = client.query('test', 'demo')   
  query.select('ltv')
  query.where(p.equals('phone', phone_number))
  ltvs = []
  def matched_names((key, metadata, bins)):
    ltvs.append(bins['ltv'])
  
  query.foreach(matched_names,  {'total_timeout':2000} )
  return ltvs


#test
add_customer(1, 12345, 30)
add_customer(2, 12345, 50)
add_customer(3, 55555, 70)

print(get_ltv_by_id(1))
print('===============')
print(get_ltv_by_id(2))
print('===============')
print(get_ltv_by_phone(12345))
print('===============')
print(get_ltv_by_phone(55555))
print('===============')

key = ('test', 'demo', 2) 
(key, metadata, record) = client.get(key)






# Close the connection to the Aerospike cluster
client.close()












