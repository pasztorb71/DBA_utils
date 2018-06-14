"""This module can rebuild indexes parallel and shows the progress graphically
It uses the general parallel_command_executor class located in
parallel_command_executor_class.py file"""

from DbUtils import getDBConnection, get_table_indexes
from data import index_rebuild
from parallel_command_executor_class import parallel_command_executor


def generate_rebuild_commands(ilist):
  tmplist = []
  for index in ilist:
    command = 'alter index ' + index[0] + '.' + index[1] + ' rebuild'
    taskname = index[0] + '.' + index[1]
    tmplist.append((command, taskname))
  return tmplist

if __name__ == '__main__':
  # Read sensitive data from a separate dictionary
  host = index_rebuild['host']
  db = index_rebuild['db']
  schema = index_rebuild['schema']
  table = index_rebuild['table']
  # End variable assignment

  title = 'Rebuild indexes'
  max_parallel_threads = 10

  """It uses my own method, but it must be replaced by somethin like this:
  con = cx_Oracle.connect(db['user'], db['pass'], dsn_tns, mode=md)"""

  con = getDBConnection(host, db)

  """This line is for getting unusable indexes on a table"""
  #ilist = get_unusable_indexes(con, schema, table)

  """ilist is a list of tuples, must look like this:
  ilist = [(schema_1, indexname_1), ..., (schema_n, indexname_n)]"""

  ilist = get_table_indexes(con, schema, table)

  """command_list is a list of tuples, must look like this:
  command_list = [(command_1, taskname_1), ..., (command_n, taskname_n)]"""

  command_list = generate_rebuild_commands(ilist)

  if not command_list:
    print('Nothing to do')
    exit(0)
  executor = parallel_command_executor(host, db, max_parallel_threads, command_list, title)
