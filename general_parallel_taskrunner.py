import threading
import time as tim
from random import randint
from tkinter import Tk, Label, ttk, Frame, Scrollbar, Canvas


def new_process_parameter_dict(id, task):
  return {'id'          : id,
          'task_name'   : 'Task_'+str(task),
          'run_flag'    : 'Waiting',
          'task_label'  : None,         #task label object
          'progressbar' : None,         #progressbar object
          'status_label': None,         #status label object
          'status_pct'  : 0,
          'command'     : ''            #It can be used by executor when sql commands to be executed
         }


def get_running_processes_cnt(process_list):
  return sum(1 if n['run_flag'] == 'Running' else 0 for n in process_list)


def dispatcher(process_list, max_parallel_threads=1000):
  """Make separated executor threads as many times as many records in the process list.
  It also limits the maximum number of treads in the same time """
  for process in process_list:
    while get_running_processes_cnt(process_list) >= max_parallel_threads:
      tim.sleep(0.1)
    t = threading.Thread(target=executor, args=(process,))
    t.start()


def fill_process_list():
  """Define tasks into process_list"""
  list = []
  for l in range(1,24):
    one_process_parameters = new_process_parameter_dict(id=l, task=l)
    list.append(one_process_parameters)
  return list

def executor(process_parameters):
  """It execute a task given in process_parameters.
  In this case it emulates a progress"""
  process_parameters['run_flag'] = 'Running'

  """This part can be modified"""
  end_cnt = randint(100,200)
  status = 0
  while status < end_cnt:
    status += randint(1,5)
    process_parameters['status_pct'] = int(status/end_cnt*100)
    tim.sleep(0.013)
  """End of modifiable part"""

  process_parameters['run_flag'] = 'Finished'


def get_work_status(process):
  """This is to contain an algoritm for measuring the progress in percent (1-100).
  It must return a number between 1 - 100"""

  """Here is the checker code"""
  out = process['status_pct']
  """End of checker code"""

  return out


def init_gui(cim):
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
  init_gui.can = canvas
  init_gui.fm = frame_main

  for process in process_list:
    init_new_progressbar(frame_progress, p_length=300, p_id=process['id'], p_process=process)
  frame_progress.update_idletasks()
  p = process_list[0]
  frame_width = p['task_label'].winfo_width() + p['progressbar'].winfo_width() + p['status_label'].winfo_width()
  frame_height = p['task_label'].winfo_height() * 20
  frame_canvas.config(width=frame_width + vsb.winfo_width(), height=frame_height)
  canvas.config(width=frame_width + vsb.winfo_width(), height=frame_height)
  canvas.config(scrollregion=canvas.bbox("all"))
  canvas.update()

  init_gui.fm.bind("<Configure>", configure)

  return root

def configure(event):
  """Runs when root window height is modified by user"""
  w, h = event.width, event.height
  init_gui.can.config(height=h)


def init_new_progressbar(frame_progress, p_length, p_id, p_process):
  """Defines a new progressbar object and two labels"""

  label = Label(frame_progress, text='Task_' + str(p_id), width=15, borderwidth=2, relief="groove")
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


def has_progressbar(process):
  return (process['progressbar'] and process['status_label'])


def progress_monitoring(frame, process_list, title='Task monitor'):
  """First parameter a list containing the tasks to be monitored"""
  finished_processes = []
  while True:
    for process in [elem for elem in process_list if elem['id'] not in finished_processes]:
      process['status_label'].config(text=process['run_flag'])
      if process['run_flag'] == 'Finished':
        process['status_label'].config(text=process['run_flag'])
        process['progressbar']['value'] = 100
        finished_processes.append(process['id'])
      else:
        process['progressbar']['value'] = get_work_status(process)
    tim.sleep(0.1)
    if len(finished_processes) == len(process_list): break
  print('Finished')


if __name__ == '__main__':
  process_list = fill_process_list()
  root = init_gui('Task monitor')
  threading.Thread(target=dispatcher, args=(process_list, 5)).start()
  threading.Thread(target=progress_monitoring, args=(root, process_list, )).start()
  root.mainloop()
