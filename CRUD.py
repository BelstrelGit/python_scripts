import logging

store = {}

def add_customer(customer_id, phone_number, lifetime_value):
    store [ customer_id ]  = {'phone': phone_number, 'ltv': lifetime_value}

def get_ltv_by_id(customer_id):
    item = store.get(customer_id, {})
    if(item == {}):
     logging.error('Requested non-existent customer ' + str(customer_id))
    else:
     return item.get('ltv')

def get_ltv_by_phone(phone_number):
    for v in store.values():
        if (v['phone'] == phone_number):
            return v['ltv']

add_customer(1, 12345, 30)

print(get_ltv_by_id(1))



