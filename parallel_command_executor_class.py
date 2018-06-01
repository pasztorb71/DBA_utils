import threading
import time as tim
from random import randint
from tkinter import Tk, Label, ttk, Frame, Scrollbar, Canvas

from DbUtils import getDBConnection, _dbQuery_conn, get_current_session, _dbExec_conn, get_unusable_indexes, \
  get_table_indexes

class parallel_command_executor():


  def __init__(self,
               host,
               database,
               max_parallel_threads=1000,
               command_list=[],
               title='Parallel tasks'):

    self.host = host
    self.database = database
    self.con = getDBConnection(host, database)
    if not command_list:
      print('Nothing to do')
      exit(0)
    self.process_list = self.fill_process_list(command_list)
    self.root = self.init_gui(title)
    threading.Thread(target=self.dispatcher, args=(self.process_list, max_parallel_threads)).start()
    threading.Thread(target=self.progress_monitoring, args=(self.root, self.process_list, )).start()
    self.root.mainloop()

  def new_process_parameter_dict(self, id, task, command):
    return {'id'          : id,
            'task_name'   : task,
            'run_flag'    : 'Waiting',
            'task_label'  : None,         #task label object
            'progressbar' : None,         #progressbar object
            'status_label': None,         #status label object
            'status_pct'  : 0,
            'command'     : command,      #It can be used by executor when sql commands to be executed
            'session'     : ''            #process(instance, SID, #serial)
           }


  def get_running_processes_cnt(self, process_list):
    return sum(1 if n['run_flag'] == 'Running' else 0 for n in process_list)


  def dispatcher(self, process_list, max_parallel_threads=1000):
    """Make separated executor threads as many times as many records in the process list.
    It also limits the maximum number of treads in the same time """
    for process in process_list:
      while self.get_running_processes_cnt(process_list) >= max_parallel_threads: tim.sleep(0.1)
      t = threading.Thread(target=self.executor, args=(process,))
      #while process['run_flag'] != 'Running': tim.sleep(0.1)
      t.start()


  def fill_process_list(self, command_list):
    """Define tasks into process_list"""
    list = []
    for index, command in enumerate(command_list):
      tmp = command.split(" ")
      task_name = tmp[2] + ' ' + tmp[3]
      one_process_parameters = self.new_process_parameter_dict(id=index, task=task_name, command=command)
      list.append(one_process_parameters)
    return list


  def get_current_session(self, con):
    """A paraméterként megadott kapcsolat (inst_id, SID, SERIAL#) -ját adja vissza tuple-ként"""
    sqlcmd = "SELECT inst_id, SID, SERIAL# FROM GV$SESSION WHERE sid=(select sid from v$mystat where rownum=1)"
    out = _dbQuery_conn(con, sqlcmd)
    return out[0]


  def executor(self, process_parameters):
    """It execute a task given in process_parameters.
    In this case it emulates a progress"""
    process_parameters['run_flag'] = 'Running'

    """This part can be modified"""
    try:
      con = getDBConnection(self.host, self.database)
      process_parameters['session'] = get_current_session(con)
      out = _dbExec_conn(con, process_parameters['command'])
      con.close()
    except Exception as e:
      print("Hiba:%s" % str(e))
    """End of modifiable part"""

    process_parameters['run_flag'] = 'Finished'


  def get_work_status(self, process):
    """This is to contain an algoritm for measuring the progress in percent (1-100).
    It must return a number between 1 - 100"""

    """Here is the checker code"""
    if not process['session']:
      return 0
    sqlcmd = "select sofar/totalwork*100 from gv$session_longops " \
             "where inst_id=" + str(process['session'][0]) \
             + " and sid=" + str(process['session'][1]) \
             + " and serial#=" + str(process['session'][2])
    out = _dbQuery_conn(self.con, sqlcmd)
    if out:
      return out[0]
    return None
    """End of checker code"""

    return out


  def init_gui(self, cim):
    """Make a new tkinter window"""
    root = Tk()
    root.grid_rowconfigure(0, weight=1)
    root.columnconfigure(0, weight=1)
    root.title(cim)
    root.resizable(False, True)
    frame_main = Frame(root)
    frame_main.grid(sticky='news')

    # Create a frame for the canvas with non-zero row&column weights
    frame_canvas = Frame(frame_main)
    frame_canvas.grid(row=0, column=0)
    frame_canvas.grid_rowconfigure(0, weight=1)
    frame_canvas.grid_columnconfigure(0, weight=1)
    # Set grid_propagate to False to allow 5-by-5 buttons resizing later
    frame_canvas.grid_propagate(False)

    # Add a canvas in that frame
    canvas = Canvas(frame_canvas)
    canvas.grid(row=0, column=0)
    canvas.pack(fill="both", expand=True)


    # Link a scrollbar to the canvas
    vsb = Scrollbar(frame_canvas, orient="vertical", command=canvas.yview)
    vsb.grid(row=0, column=1, sticky='ns')
    canvas.configure(yscrollcommand=vsb.set)

    # Create a frame to contain the scrollable content
    frame_progress = Frame(canvas)
    canvas.create_window((0, 0), window=frame_progress, anchor='nw')
    self.canvas = canvas
    self.frame_main = frame_main

    for process in self.process_list:
      self.init_new_progressbar(frame_progress, p_length=300, p_process=process)
    frame_progress.update_idletasks()
    p = self.process_list[0]
    frame_width = p['task_label'].winfo_width() + p['progressbar'].winfo_width() + p['status_label'].winfo_width()
    rows = 20 if len(self.process_list) > 20 else len(self.process_list)
    frame_height = (p['progressbar'].winfo_height() * rows)
    frame_canvas.config(width=frame_width + vsb.winfo_width(), height=frame_height)
    canvas.config(width=frame_width + vsb.winfo_width(), height=frame_height)
    canvas.config(scrollregion=canvas.bbox("all"))
    canvas.update()

    self.frame_main.bind("<Configure>", self.configure)

    return root

  def configure(self, event):
    """Runs when root window height is modified by user"""
    w, h = event.width, event.height
    self.canvas.config(height=h)


  def init_new_progressbar(self, frame_progress, p_length, p_process):
    """Defines a new progressbar object and two labels"""

    p_id = p_process['id']
    task_name = p_process['task_name']

    label = Label(frame_progress, text=task_name, width=len(task_name), borderwidth=2, relief="groove")
    label.grid(row=p_id, column=0)
    p_process['task_label'] = label
    progress = ttk.Progressbar(frame_progress, orient="horizontal", length=p_length, mode="determinate")
    progress.grid(row=p_id, column=1)
    progress['maximum'] = 100
    progress['value'] = 0

    p_process['progressbar'] = progress
    label = Label(frame_progress, text='Waiting', width=15, borderwidth=2, relief="groove")
    label.grid(row=p_id, column=2)
    p_process['status_label'] = label

    return progress


  def has_progressbar(self, process):
    return (process['progressbar'] and process['status_label'])


  def progress_monitoring(self, frame, process_list, title='Task monitor'):
    """First parameter a list containing the tasks to be monitored"""
    finished_processes = []
    while True:
      for process in [elem for elem in process_list if elem['id'] not in finished_processes]:
        process['status_label'].config(text=process['run_flag'])
        if process['run_flag'] == 'Finished':
          process['status_label'].config(text=process['run_flag'])
          process['progressbar']['value'] = 100
          finished_processes.append(process['id'])

        if process['run_flag'] == 'Running':
          stat = self.get_work_status(process)
          if stat:
            process['progressbar']['value'] = stat
      tim.sleep(0.1)
      if len(finished_processes) == len(process_list): break
    print('Finished')

