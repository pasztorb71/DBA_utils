"""This module can rebuild indexes parallel and shows the progress graphically
It uses the general parallel_command_executor class located in
parallel_command_executor_class.py file"""

from DbUtils import getDBConnection, get_table_indexes
from data import index_rebuild
from parallel_command_executor_class import parallel_command_executor


def generate_rebuild_commands(ilist):
  tmplist = []
  for index in ilist:
    tmplist.append('alter index ' + index[0] + '.' + index[1] + ' rebuild')
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

  con = getDBConnection(host, db)
  #ilist = get_unusable_indexes(con, schema, table)
  ilist = get_table_indexes(con, schema, table)
  command_list = generate_rebuild_commands(ilist)
  if not command_list:
    print('Nothing to do')
    exit(0)
  executor = parallel_command_executor(host, db, max_parallel_threads, command_list, title)
