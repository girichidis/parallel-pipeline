# simple test program that shows the content of the data base

# Philipp Girichidis, 2019-03-10

import sqlite3 as lite
from DBspecs import *
import os

import argparse
parser = argparse.ArgumentParser(description='parallel pipeline') 
parser.add_argument('-dbtable', type=str, help="SQlite DB file", default=dbtable)
parser.add_argument('-db',      type=str, help="SQlite DB file", default=database)
args = parser.parse_args()

if not os.path.isfile(args.db):
    print("")
    print("database ", args.db, " does not exist! EXIT!!")
    print("")
    exit()
try:
    con = lite.connect(args.db)
except:
    print("")
    print("cannot connect to database: ", args.db, " EXIT!!")
    print("")
    exit()
    
with con:
    cur = con.cursor()    
    # check whether table exists
    try:
        cur.execute("SELECT NULL FROM "+args.dbtable+" LIMIT 1")
    except:
        print("") 
        print("DB table ", args.dbtable, " does not exist. EXIT!!")
        print("")
        exit()
        
    db_cmd = "SELECT rowid, "+', '.join(db_field_names)+" FROM "+args.dbtable
    print (db_cmd)
    cur.execute(db_cmd)
    rows = cur.fetchall()
    for row in rows:
        print(row)
