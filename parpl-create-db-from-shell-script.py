# helper program that creates the database with all the commands and command results

# Philipp Girichidis, 2019-03-10

import sqlite3 as lite

import argparse
parser = argparse.ArgumentParser(description='Plotting script') 
parser.add_argument('files',      nargs='+',   help='time evol data file')
parser.add_argument('-show_cmd',   action="store_true")
args = parser.parse_args()

from DBspecs import *

# try to connect to sqlite db
try:
    con = lite.connect(database)
    cur = con.cursor()    
    cur.execute('SELECT SQLITE_VERSION()')
    data = cur.fetchone()
    print ("SQLite version: %s" % data)                
except lite.Error:   
    print ("Error")
    exit()
finally:
    if con:
        con.close()

# create a new DB with the commands
con = lite.connect(database)

with con:
    cur = con.cursor()

    # removing old table if it exists
    cur.execute("drop table if exists "+dbtable)

    # create new table
    db_cmd = "CREATE TABLE "+dbtable+" ("+', '.join(db_fields)+")"
    print("Generate new table")
    print(db_cmd)
    cur.execute(db_cmd)
    
    
    # read commands and put them in db table
    with open(args.files[0]) as f:
        print("reading commands from file")
        for line in f:
            # check if comments
            if line[0] != "#" and len(line) > 2:
                # analyse line by splitting
                parts = line.rstrip('\n').split(";")
                db_cmd = "INSERT INTO "+dbtable+" ("+', '.join(db_field_names[:len(parts)])\
                         +") VALUES ("+', '.join("'" + item + "'" for item in parts)+")" 
                print (db_cmd)
                cur.execute(db_cmd)
# commit changes
#con.commit()
            
    # re-read db to check that is worked
    db_cmd = "SELECT "+', '.join(db_field_names)+" FROM "+dbtable
    print (db_cmd)
    cur.execute(db_cmd)
    rows = cur.fetchall()
    for row in rows:
        print(row)
#con.close()


