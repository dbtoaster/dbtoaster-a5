#!/usr/bin/python

import os, os.path, shutil, signal, subprocess, time
from csv import DictReader
from sys import argv
from optparse import OptionParser

default_dir = "/damsl/software/streambase"
event_file_name = "file_events.csv"

queries = {
  'query3'      : ['lineitem', 'orders', 'customer'], 
  'query17'     : ['lineitem', 'part'],
  'query18'     : ['lineitem', 'orders', 'customer'],
  'query22'     : ['orders', 'customer'],
  'vwap'        : ['events'],
  'axfinder'    : ['events'],
  'pricespread' : ['events'],
  'missedtrades': ['events']
}

num_files = {}
for qn in queries:
  num_files[qn] = len(queries[qn])

# Check if SPE's CSV adaptor is done with all files for the given query.
#####
def check_terminate(query_name):
  done = False
  try:
    with open(event_file_name) as evt_file:
      reader = DictReader(evt_file)
      files_closed = 0
      for row in reader:
        if row['Type'] == 'Close':
          cf = os.path.splitext(os.path.basename(row['Object']))[0].lower()
          if query_name in queries and cf in queries[query_name]:
            files_closed += 1
      done = files_closed == num_files[query_name]
  except IOError:
    print event_file_name+" not found... continuing to poll"
  return done

# Called after termination to save any experiment outputs
def save_run(query_name):
  if os.path.exists(event_file_name):
    shutil.move(event_file_name, query_name+"_"+event_file_name)

# Run SPE in a subprocess, poll for termination based on file events,
# send SIGTERM to terminate the SPE
#####
def run_stream_engine(sbdir, query_name, loader_name, poll_period, timeout):
  terminate = False
  (original, loader) = query_name+".ssql", query_name+loader_name+".ssql"
  query_file = loader if loader != "" else original

  # Spawn and poll.
  spe_log_filename = query_name+"_run.log"
  spe_cmd = ' '.join([os.path.join(sbdir, "bin/run_streambase.sh"), \
                      query_file, '>{0}', '2>&1', ]).format(spe_log_filename)
  print "Starting SPE with '{0}'".format(spe_cmd)
  spe = subprocess.Popen(spe_cmd, shell=True)
  
  time.sleep(5)
  with open(spe_log_filename) as log_file:
    spe_pid = int(log_file.readline()) 
  print "SPE pid {0}".format(spe_pid)

  start = time.time()
  current = start
  while not(terminate) and (current-start) < timeout:
    remaining = timeout-(current-start)
    sleep_duration = poll_period if remaining >= poll_period else remaining
    time.sleep(sleep_duration)
    done = spe.poll() 
    current = time.time()
    terminate = check_terminate(query_name) or (done != None) or (current-start) >= timeout
  
  # Cleanup.
  print "SPE done, terminating..."
  if spe.poll() == None:
    os.kill(spe_pid, signal.SIGKILL)
    spe.terminate()
  save_run(query_name)

if __name__ == '__main__':
  usage = "Usage: %prog [options] <query name>"
  parser = OptionParser(usage=usage)
  
  parser.add_option("-p", "--poll", type="int", dest="poll_period", default=10,
                    help="set termination polling period", metavar="SECS")

  parser.add_option("-l", "--loader", dest="loader", default="",
                    help="use query loader", metavar="LOADER")
  
  parser.add_option("-d", "--sbdir", dest="sbdir", default=default_dir,
                    help="set SPE top-level dir", metavar="DIR")
                    
  parser.add_option("-t", "--timeout", dest="timeout", type=float, default=6000.0,
                    help="set execution timeout", metavar="SECS")
                    
  (options, args) = parser.parse_args()
  
  if len(args) < 1:
    parser.error("no query specified")
  
  # Check SPE binaries  
  test_sb_paths = [options.sbdir, os.path.join(options.sbdir, "bin/sbd")]
  if not(all([os.path.exists(p) for p in test_sb_paths])):
    parser.error("invalid SPE directory: "+options.sbdir)

  query_name = args[0]
  query_loader = options.loader 
  poll_period = options.poll_period
  timeout = options.timeout

  # Check query files
  test_q_paths = [query_name+".ssql"] + \
    ([query_name+query_loader+".ssql"] if query_loader != "" else [])

  if query_name in queries and all([os.path.exists(p) for p in test_q_paths]):
    print "Running query " + query_name + " from " + os.getcwd()
    run_stream_engine(options.sbdir, query_name, query_loader, poll_period, timeout)
  else:
    print "Unknown query: " + query_name
    print "Valid queries: " + str(queries.keys())
  