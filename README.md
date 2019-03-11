# parallel-pipeline
MPI parallel pipeline for small serial jobs

## Motivation
Often I need to run many small individual serial jobs. This can be conveniently done in a large MPI parallel job with the help of e.g., GNU parallel. However, GNU parallel is missing some features that I like to add with this small set of python programs:
 - store commands including exec time (and memory reqirements) in database
 - mark completed jobs, such that restart does not run commands twice
 - use SQL-like database instead of a weird collection of ASCII files
 - optional: do not run command if a specified result file is present 
 
The goal is to have a DB at the end will all serial jobs, the total exec time, the individual memory requirement

## Description
The master process reads all commands from the database and distributes them to the other MPI threads. Using asynchronous communication the master process waits for any process to be finished and distributes the new task. Currently the MPI processes only return the time that the task needed to be executed.

## first version
This is only the first version and is not really tested...

## TODO
 - allow mutiple tables in DB and multiple DBs
 - include memory requirements and automatically limit the MPI tasks per node according to mem.
 
