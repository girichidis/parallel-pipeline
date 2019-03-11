# simple test program that shows the content of the data base

# Philipp Girichidis, 2019-03-10

import sqlite3 as lite
from DBspecs import *

con = lite.connect(database)
with con:
    cur = con.cursor()    
    db_cmd = "SELECT rowid, "+', '.join(db_field_names)+" FROM "+dbtable
    print (db_cmd)
    cur.execute(db_cmd)
    rows = cur.fetchall()
    for row in rows:
        print(row)
