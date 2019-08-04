'''
@Author: pvishnoi
@Date: Aug 4 2019
@Description: git init API TO Sqlite
'''

import json
import sqlite3
from sqlite3 import Error
import chargebee


#################################### Downloading data as JSON format ################################
#####################################################################################################
# write json data into subscription.json
def write_json_subscription():
    subscrs = chargebee.Subscription.list({"sort_by[asc]": "created_at"})
    for subscr in subscrs:
        temp = subscr.__str__()
        with open('subscription.json', 'a') as fh:
            fh.write(temp)


# write json data into invoice.json
def write_json_invoice():
    invoices = chargebee.Invoice.list({"sort_by[asc]": "date"})
    for invoice in invoices:
        temp = invoice.__str__()
        with open('invoice.json', 'a') as fh:
            fh.write(temp)


#################################### Database Connection ##############################################
#######################################################################################################

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        print('sqlite3 version: ', sqlite3.version)
        print('Successfully connected %s database' % (db_file))
        return conn
    except Error as e:
        print(e)

    return None


#################################### Subscription and Creation of Tables #############################
######################################################################################################

def create_table_subscription(conn):
    """ create a table schema from the chargebee.Subscription.fields list
    :param conn: Connection object
    :return:
    """

    # type specifier tokens for integer, boolean and date
    int_list = ['quantity', 'price', 'fee', 'amount', 'period', 'unit', 'cycles', 'version', 'count', \
                'dues', 'total', 'has_', 'deleted', 'relationship', 'since', 'date', 'start', 'end', '_at']

    # get the fields of Subcription module
    fields = chargebee.Subscription.fields

    # type specifier tokens for REAL values
    real_list = ['mrr', 'exchage_rate']

    # Beginning of the SQL statement
    sql = 'CREATE TABLE IF NOT EXISTS subscription ( '
    foreign_key = ''

    # Assign dynamically the field names with datatypes and field size in the table
    for field in fields:
        # fix primary keys
        if field == 'id':
            sql += field + ' VARCHAR(50) PRIMARY KEY, '

        elif '_id' in field:
            if field == 'gift_id':
                sql += field + ' VARCHAR(150), '

            elif field == 'plan_id':
                sql += field + ' VARCHAR(100), '

            elif field == 'payment_source_id':
                sql += field + ' VARCHAR(40), '

            else:
                sql += field + ' VARCHAR(50) NOT NULL, '

            # Fix foreign keys
            foreign_key += 'FOREIGN KEY (%s) REFERENCES %s (id), ' % (field, field[:-3])

        elif field in real_list:
            sql += field + ' REAL, '

        elif field == 'meta_data':
            sql += field + ' BLOB, '

        elif field == 'po_number':
            sql += field + ' VARCHAR(100), '

        elif field == 'created_from_ip':
            sql += field + ' VARCHAR(50), '

        elif field == 'affiliate_token':
            sql += field + ' VARCHAR(200), '

        elif field == 'invoice_notes':
            sql += field + ' VARCHAR(1000), '

        elif 'code' in field:
            sql += field + ' VARCHAR(3),'

        # fix field data types: integer, text, boolean, date
        elif any(i in field for i in int_list):
            sql += field + ' INTEGER, '

        else:
            sql += field + ' TEXT, '

    sql += foreign_key[:-2] + ' );'

    print(sql)

    try:
        c = conn.cursor()
        c.execute(sql)
        insert_new_field(conn, 'subscription', 'object', 'TEXT')
        print('Table: subscription is created successfully')

    except Error as e:
        print(e)


#################################### Invoice Table ##############################################
#################################################################################################

def create_table_invoice(conn):
    """ create a table schema from the chargebee.Subscription.fields list
    :param conn: Connection object
    :return:
    """
    # type specifier tokens for integer, boolean and date
    int_list = ['recurring', 'date', '_at', 'days', 'total', 'amount', 'version', \
                'tax', 'invoice', 'has_', 'is_', 'finalized', 'deleted']

    # get the fields of Subcription module
    fields = chargebee.Invoice.fields
    # Beginning of the SQL statement
    sql = 'CREATE TABLE IF NOT EXISTS invoice ( '
    # foreign key string
    foreign_key = ''
    # Assign dynamically the field names with datatypes and field size in the table
    for field in fields:
        # fix primary keys
        if field == 'id':
            sql += field + ' VARCHAR(50) PRIMARY KEY, '

        elif '_id' in field:
            sql += field + ' VARCHAR(50) NOT NULL, '
            foreign_key += 'FOREIGN KEY ({}) REFERENCES {} (id), '.format(field, field[:-3])

        elif field == 'po_number':
            sql += field + ' VARCHAR(100), '

        elif field == 'vat_number':
            sql += field + ' VARCHAR(20), '

        elif field == 'credits_applied':
            sql += field + ' INTEGER, '

        elif field == 'currency_code':
            sql += field + ' VARCHAR(3) NOT NULL,'

        # fix field data types: integer, text, boolean, date
        elif any(i in field for i in int_list):
            sql += field + ' INTEGER, '

        else:
            sql += field + ' TEXT, '

    sql += foreign_key[:-2] + ' );'

    try:
        c = conn.cursor()
        c.execute(sql)
        insert_new_field(conn, 'invoice', 'exchange_rate', 'REAL')
        insert_new_field(conn, 'invoice', 'object', 'TEXT')
        insert_new_field(conn, 'invoice', 'base_currency_code', 'VARCHAR(3)')
        insert_new_field(conn, 'invoice', 'new_sales_amount', 'INTEGER')
        insert_new_field(conn, 'invoice', 'is_digital', 'INTEGER')
        print('Table: invoice is created successfully')

    except Error as e:
        print(e)


#################################### Inserting New / Missing Columns ################################
#####################################################################################################

def insert_new_field(conn, table_name, col_name, col_type):
    sql = 'ALTER TABLE %s ADD COLUMN %s %s;' % (table_name, col_name, col_type)

    try:
        c = conn.cursor()
        c.execute(sql)
        print('A new column {} is added in {} tables uccessfully'.format(col_name, table_name))

    except Error as e:
        print(e)


#################################### Insertion of Data in Tables ######################################
#######################################################################################################

def insert_into_table(conn, table_name):
    if table_name == 'subscription':
        entries = chargebee.Subscription.list({"sort_by[asc]": "created_at"})

    elif table_name == 'invoice':
        entries = chargebee.Invoice.list({"sort_by[asc]": "date"})

    key = 'subscription' if table_name == 'subscription' else 'invoice'

    for entry in entries:
        json_dict = json.loads(entry.__str__())[key]
        fields = ', '.join(json_dict.keys())
        placeholders = ':' + ', :'.join(json_dict.keys())
        sql = 'INSERT INTO {} ({}) VALUES ({})'.format(table_name, fields, placeholders)
        for k, v in json_dict.items():
            if (type(v) == type([])) or type(v) == type({}):
                json_dict[k] = json.dumps(v)
        print(json_dict, sql)

        try:
            curr = conn.cursor()
            curr.execute(sql, json_dict)
            conn.commit()
            print('Successfully inserted into %s table' % (table_name))
        except Error as e:
            print(e)


#################################### Chargebee API Key Config ######################################
####################################################################################################

# initialize the chargebee api key configuration
def init():
    api_key = 'test_jE0siZmYhi5XPkN4zNQkNANSchc7qFPg'
    base_url = 'personio-old-test'
    chargebee.configure(api_key, base_url)


#################################### Main Function ##############################################
#################################################################################################

# Main function
def main():
    # Initialize the api key to the chargebee configuration
    init()

    # Enter the name of new database
    db_file = input('Create or Enter the database name: ')
    # create a connection the with the database
    conn = create_connection(db_file)

    # create tables: subscription and invoice
    if conn is not None:
        # create subscription table and invoice table
        create_table_subscription(conn)
        create_table_invoice(conn)
    else:
        print("Error! cannot create the database connection.")
    # INSERT data into tables - subscription and invoice
    insert_into_table(conn, 'subscription')
    insert_into_table(conn, 'invoice')
    conn.close()


#################################### Program Starts ##############################################
##################################################################################################

if __name__ == '__main__':
    main()
