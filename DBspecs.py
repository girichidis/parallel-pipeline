# DB specifications for parpl
# fo far the table and the DB name are hard-coded. This must change
# Philipp Girichidis, March 2019

database = "parpl.db"
dbtable  = "cmdtable"

# db_fields = {
#     "cmd": "text",
#     "resultfile": "text",
#     "cmdtype": "int",
#     "cmdexe": "int",
#     "exetime": "int",
#     "comment": "text"
#     }

# do not use a dict because a dict does not have an order, which we need later
db_field_names = ["cmd", "resultfile", "cmdtype", "cmdexe", "exetime", "comment"]
db_field_types = ["text", "text", "int", "int", "int", "text"]
db_fields = ["{} {}".format(a_, b_) for a_, b_ in zip(db_field_names, db_field_types)]
