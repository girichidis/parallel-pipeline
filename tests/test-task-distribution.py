# this is an independent test program to illustrate and test the 
# distribution of tasks with an asynchronous communication

# Philipp Girichidis, 2019-03-10

from mpi4py import MPI
import subprocess
import time
import numpy as np
import shlex

comm = MPI.COMM_WORLD
size = comm.size
rank = comm.rank

#print ("initial test, waiting 5 seconds")
#subprocess.run(["sleep", "1"])

requests = []

Nmax=10

# list that stores, which process is busy
busy = [False]*size
#busy[0] = True
print(busy)

global_time_start = time.time()

if rank == 0:
    cnt = 0

    # (1) initial distribution
    print("INITIAL PHASE, DISTRIBUTION")
    print("rank 0 is sending...")
    # take care that size is not longer than number of tasks
    for i in range(1,min(size,Nmax+1)):
        # data structure is list with command being one string
        # executing rank splits that for subprocess
        data = ["sleep "+str(i**3)]
        print("rank 0 sending to rank ", i, data, " this is count ", cnt)
        req = comm.isend(data, dest=i, tag=11)
        requests.append(req)
        req.Wait()
        cnt += 1
        busy[i] = True
        
    # (2) waiting for processes, redistributing new work
    print("SECOND PHASE, NEW DISTRIBUTION")
    while cnt < Nmax:
        # now all commands have been sent, so wait for any respose
        s = MPI.Status()
        while not comm.Iprobe(source=MPI.ANY_SOURCE, tag=11, status=s):
            #print ("        -- rank 0 is waiting...")
            time.sleep(0.5)
        print("rank 0 got messsage from rank ", s.source)
        result = comm.recv(source=s.source, tag=11)
        print("info from rank 0: rank ", s.source, " took", result[1], " time")

        data = ["sleep "+str(s.source**3)]
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
        print("info from rank 0: rank ", s.source, " took", result[1], " time")

        data = ["STOP"]
        print("rank 0 sending STOP rank ", s.source, data, " this is count ", cnt)
        req = comm.isend(data, dest=s.source, tag=11)
        requests.append(req)
        req.Wait()
        busy[s.source] = False
        
        
else:
    # take care of the possibility that Nmax is small
    if rank <= Nmax:
        cmds = comm.recv(source=0, tag=11)
        #print("rank ", rank, " receiving command: ", cmds)
        while cmds[0] != "STOP":
            # prepare executable command
            run_cmd = shlex.split(cmds[0])
            #print(run_cmd)
            
            # measure time for the command
            start = time.time()
            subprocess.run(run_cmd)
            end = time.time()
            #print("rank ", rank, " done waiting, took ", end-start, "seconds")
            exectime = end-start

            # prepare data to send back
            result = [1, exectime]
            comm.send(result, dest=0, tag=11)
            
            # done with command, read next one!
            cmds = comm.recv(source=0, tag=11)
            #print("rank ", rank, " receiving command: ", cmds)
        else:
            print("rank ", rank, " got STOP command")

global_time_end = time.time()
print("GLOBAL TIME FOR RANK ", rank, global_time_end-global_time_start)
comm.Barrier()
