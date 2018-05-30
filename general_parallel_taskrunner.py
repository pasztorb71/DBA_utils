import threading
import time as tim
from random import randint
from tkinter import Tk, Label, ttk


def new_process_parameter_dict(id, task):
  return {'id'          : id,
          'task_name'   : 'Task_'+str(task),
          'run_flag'    : 'Running',
          'progressbar' : None,         #progressbar object
          'status_label': None,         #status label object
          'status_pct'  : 0
         }


def dispatcher(process_list, max_parallel_threads=0):
  """Define tasks and calls the executor many times in separated threads"""
  thread_counter = 1
  for l in range(1,14):
    one_process_parameters = new_process_parameter_dict(id=thread_counter, task=l)
    running_processes_cnt = sum(1 if n['run_flag']=='Running' else 0 for n in process_list)
    while running_processes_cnt >= max_parallel_threads:
      tim.sleep(0.1)
      running_processes_cnt = sum(1 if n['run_flag']=='Running' else 0 for n in process_list)
    process_list.append(one_process_parameters)
    t = threading.Thread(target=executor, args=(one_process_parameters,))
    t.start()
    thread_counter += 1

def executor(process_parameters):
  """It execute a task.
  In this case it emulates a progress"""
  end_cnt = randint(100,200)
  status = 0
  while status < end_cnt:
    status += randint(1,5)
    process_parameters['status_pct'] = int(status/end_cnt*100)
    tim.sleep(0.01)
  process_parameters['run_flag'] = 'Finished'

def get_work_status(process):
  """This is to contain an algoritm for measuring the progress in percent (1-100)"""
  return process['status_pct']


def init_gui(cim):
  """Make a new tkinter window"""
  root = Tk()
  root.title(cim)
  return root


def init_new_progressbar(f, p_length, p_id, p_process):
  label = Label(f, text='Task_'+str(p_id), width=15)
  label.grid(row=p_id, column=0)
  progress = ttk.Progressbar(f, orient="horizontal", length=p_length, mode="determinate")
  progress.grid(row=p_id, column=1)
  progress['maximum'] = 100
  progress['value'] = 0

  p_process['progressbar'] = progress
  label = Label(f, text='Running', width=15)
  label.grid(row=p_id, column=3)
  p_process['status_label'] = label

  return progress


def wait_until_process_list_is_empty(process_list):
  while not process_list: tim.sleep(0.1)


def has_progressbar(process):
  return (process['progressbar'] and process['status_label'])


def progress_monitoring(process_list, title='Task monitor'):
  """First parameter a list containing the tasks to be monitored"""

  f = init_gui(title)
  wait_until_process_list_is_empty(process_list)

  finished_processes = []
  while True:
    if_ended = False
    for process in [elem for elem in process_list if elem['id'] not in finished_processes]:
      if not has_progressbar(process): init_new_progressbar(f, p_length=300, p_id=process['id'], p_process=process)

      if process['run_flag'] == 'Finished':
        process['status_label'].config(text=process['run_flag'])
        finished_processes.append(process['id'])
      else:
        status = get_work_status(process)
        if status:
          process['progressbar']['value'] = status
          if_ended = True
    tim.sleep(0)
    f.update()
    if not if_ended: break

  print('Finished')
  f.mainloop()

process_list = []
threading.Thread(target=progress_monitoring, args=(process_list,)).start()
dispatcher(process_list, max_parallel_threads=5)
