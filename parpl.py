# main program that reads the commands from the database and
# distributes the work among all the MPI processes

# Philipp Girichidis, March 2019

# imports
from mpi4py import MPI
import argparse
import sqlite3 as lite
import os
import shlex
import time
import subprocess

from DBspecs import *

def command_done(row):

    # check whether resultfile exists, add 1 to column, because of rowid
    file_exists = (row[db_field_names.index("resultfile")+1] != None) and (os.path.isfile(row[db_field_names.index("resultfile")+1]))
    #print("checking for file ", row[db_field_names.index("resultfile")])
    #if os.path.isfile(row[db_field_names.index("resultfile")]):
    #    print("file found")
    cmd_done = row[db_field_names.index("cmdexe")+1] == 1
    return file_exists or cmd_done
    

parser = argparse.ArgumentParser(description='parallel pipeline') 
parser.add_argument('-dbtable', type=str, help="SQlite DB file", default=dbtable)
parser.add_argument('-db',      type=str, help="SQlite DB file", default=database)
parser.add_argument('-show_cmd',   action="store_true")
args = parser.parse_args()


# set up MPI
comm = MPI.COMM_WORLD

rank = comm.rank
size = comm.size

busy = [False]*size
requests = []

# if this is rank 0 then do the organisation
if comm.rank == 0:
    print("This is rank 0, I read the commands from the database")
    if not os.path.isfile(args.db):
        print("")
        print("cannot find database: ", args.db, " EXIT!!")
        print("")
        comm.Abort(1)
    try:
        con = lite.connect(args.db)
        cur = con.cursor()    
        cur.execute('SELECT SQLITE_VERSION()')
        data = cur.fetchone()
        print ("SQLite version: %s" % data)                
    except lite.Error:   
        print("")
        print("cannot connect to database: ", args.db, " EXIT!!")
        print("")
        comm.Abort(1)
    
    with con:
        # check whether table exists
        try:
            cur.execute("SELECT NULL FROM "+args.dbtable+" LIMIT 1")
        except:
            print("") 
            print("DB table ", args.dbtable, " does not exist. EXIT!!")
            print("")
            comm.Abort(1)

        # read list of tasks from db
        db_cmd = "SELECT rowid, "+', '.join(db_field_names)+" FROM "+args.dbtable
        print (db_cmd)
        cur.execute(db_cmd)
        rows = cur.fetchall()
        
        # generate an index list with all indices of lines that need to be executed
        exec_idx = []
        for i in range(len(rows)):
            if not command_done(rows[i]):
                exec_idx.append(i)

        print (exec_idx)
        
        # total number of tasks that need to be executed
        Ntasks = len(exec_idx)
        print("total number of tasks", Ntasks)
        
        cnt = 0

        # (1) initial distribution
        print("INITIAL PHASE, DISTRIBUTION")
        print("rank 0 is sending...")
        # take care that size is not longer than number of tasks
        for i in range(1,size):
            if i <= Ntasks:
                # data structure is list with command in one string
                # executing rank splits that for subprocess
                # command is column 1 (col 0 in rows is rowid)
                row = rows[exec_idx[cnt]]
                data = [row[0], row[1]]
            else:
                data = ["STOP"]
            print("rank 0 sending to rank ", i, data, " this is count ", cnt)
            req = comm.isend(data, dest=i, tag=11)
            requests.append(req)
            req.Wait()
            cnt += 1
            if i <= Ntasks:
                busy[i] = True
            else:
                busy[i] = False
        
        # (2) waiting for processes, redistributing new work
        print("SECOND PHASE, NEW DISTRIBUTION")
        while cnt < Ntasks:
            # now all commands have been sent, so wait for any respose
            s = MPI.Status()
            while not comm.Iprobe(source=MPI.ANY_SOURCE, tag=11, status=s):
                #print ("        -- rank 0 is waiting...")
                time.sleep(0.5)
            print("rank 0 got messsage from rank ", s.source)
            result = comm.recv(source=s.source, tag=11)
            print("info from rank 0: rank ", s.source, " with rowid ", result[0], "took", result[2], " time")

            # done with command, update DB
            db_cmd = "UPDATE "+args.dbtable+" SET exetime = "+str(int(result[2]))+", cmdexe = 1  WHERE rowid = "+str(result[0])
            cur.execute(db_cmd)
            
            row = rows[exec_idx[cnt]]
            data = [row[0], row[1]]
            print("rank 0 sending to rank ", s.source, data, " this is count ", cnt)
            req = comm.isend(data, dest=s.source, tag=11)
            requests.append(req)
            req.Wait()
            cnt += 1

        # (3) done with tasks, wait until all processes done and send them STOP
        print("THIRD PHASE: WATING AND SENDING STOP")
        while not (busy == [False]*size):
            s = MPI.Status()
            while not comm.Iprobe(source=MPI.ANY_SOURCE, tag=11, status=s):
                #print ("        -- rank 0 is waiting...")
                time.sleep(0.5)
            print("rank 0 got messsage from rank ", s.source)
            result = comm.recv(source=s.source, tag=11)
            print("info from rank 0: rank ", s.source, " with rowid ", result[0], "took", result[2], " time")

            # done with command, update DB
            db_cmd = "UPDATE "+args.dbtable+" SET exetime = "+str(int(result[2]))+", cmdexe = 1  WHERE rowid = "+str(result[0])
            cur.execute(db_cmd)

            data = ["STOP"]
            print("rank 0 sending STOP rank ", s.source, data, " this is count ", cnt)
            req = comm.isend(data, dest=s.source, tag=11)
            requests.append(req)
            req.Wait()
            busy[s.source] = False

else:
    #print("rank", comm.rank)
    # take care of the possibility that Nmax is small

    cmds = comm.recv(source=0, tag=11)
    print("rank ", rank, " receiving command: ", cmds)
    while cmds[0] != "STOP":
        # prepare executable command
        run_cmd = shlex.split(cmds[1]) # cmd[0] is rowid
        #print(run_cmd)
        
        # measure time for the command
        start = time.time()
        subprocess.run(run_cmd)
        end = time.time()
        #print("rank ", rank, " done waiting, took ", end-start, "seconds")
        exectime = end-start
        
        # prepare data to send back: rowid, done, time
        result = [cmds[0], 1, exectime]
        comm.send(result, dest=0, tag=11)
        
        # done with command, read next one!
        cmds = comm.recv(source=0, tag=11)
        #print("rank ", rank, " receiving command: ", cmds)
    else:
        print("rank ", rank, " got STOP command")
            
    
comm.Barrier()
