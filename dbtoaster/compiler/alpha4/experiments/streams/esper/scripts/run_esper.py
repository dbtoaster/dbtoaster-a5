#!/usr/bin/python

import os, os.path, shutil, signal, subprocess, time
from csv import DictReader
from sys import argv, exit
from optparse import OptionParser

default_esper_dir = "../"
default_query_dir = "../queries"

finance_queries = {
  'vwap'        : ['-r 3', '-s {SAMPLEFREQ}', '-u {EVENTS}'],
  'axfinder'    : ['-r 1', '-s {SAMPLEFREQ}', '-u {EVENTS}'],
  'brokerspread': ['-r 1', '-s {SAMPLEFREQ}', '-u {EVENTS}'],
  'pricespread' : ['-r 1', '-s {SAMPLEFREQ}', '-u {EVENTS}'],
  'missedtrades': ['-r 3', '-s {SAMPLEFREQ}', '-u {EVENTS}']
}

finance_ext = ".dbtdat"
finance_files = [i+finance_ext for i in \
                 ["Events", "InsertBIDS", "DeleteBIDS", "InsertASKS", "DeleteASKS"]]

tpch_queries = {
  'query3'   : ['-r 1', '-b {ABSDATADIR}', '-i {LINEITEM}', '-i {CUSTOMER}', '-i {ORDERS}'],
  'query17'  : ['-r 3', '-b {ABSDATADIR}', '-i {LINEITEM}', '-i {PART}'],
  'query18'  : ['-r 3', '-b {ABSDATADIR}', '-i {LINEITEM}', '-i {CUSTOMER}', '-i {ORDERS}'],
  'query22'  : ['-r 3', '-b {ABSDATADIR}', '-i {CUSTOMER}', '-i {ORDERS}'],
  'ssb4'     : ['-r 1', '-b {ABSDATADIR}', '-i {LINEITEM}', '-i {CUSTOMER}', '-i {ORDERS}', '-i {SUPPLIER}', '-i {PART}', '-i {NATION}']
}

tpch_ext = ".csv"
tpch_files = [i+tpch_ext for i in \
              ["lineitem", "orders", "customer", "supplier", "part", "partsupp", "nation", "region"]]


def run_spe(cmd, query_name, poll_period, timeout, outdir):
  terminate = False
  
  # Spawn and poll.
  log_filename = os.path.join(outdir, query_name+"_run.log")
  spe_cmd = cmd+' >{0} 2>&1'.format(log_filename)
  print "Starting SPE with '{0}'".format(spe_cmd)
  spe = subprocess.Popen(spe_cmd, shell=True)
  
  time.sleep(2)
  with open(log_filename) as log_file:
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
    terminate = (done != None) or (current-start) >= timeout
  
  # Cleanup.
  print "SPE done, terminating..."
  if spe.poll() == None:
    os.kill(spe_pid, signal.SIGTERM)
    spe.terminate()

def format_finance_args(cmd, options):
  samplefreq = options.sample
  datadir = "finance" if (options.datadir == None or options.datadir == "") else options.datadir
  files = [os.path.join(datadir, f) for f in finance_files]
  return cmd.format(SAMPLEFREQ=samplefreq, \
            EVENTS=files[0], IBIDS=files[1], DBIDS=files[2], IASKS=files[3], DASKS=files[4])

def format_tpch_args(cmd, options):
  samplefreq = options.sample
  esper_dir = options.esperdir
  datadir = "tpch" if options.datadir == "" else options.datadir
  abs_datadir = os.path.join(esper_dir, "bin")
  files = [os.path.join(datadir, f) for f in tpch_files]
  return cmd.format(SAMPLEFREQ=samplefreq, ABSDATADIR=abs_datadir, DATADIR=datadir, \
            LINEITEM=files[0], ORDERS=files[1], CUSTOMER=files[2], SUPPLIER=files[3], \
            PART=files[4], PARTSUPP=files[5], NATION=files[6], REGION=files[7])
    
if __name__ == '__main__':
  usage = "Usage: %prog [options] <query name>"
  parser = OptionParser(usage=usage)

  parser.add_option("-s", "--sample", type="int", dest="sample", default=10,
                    help="set sample frequency", metavar="#RESULTS")

  parser.add_option("-p", "--poll", type="int", dest="poll_period", default=10,
                    help="set termination polling period", metavar="SECS")

  parser.add_option("-t", "--timeout", dest="timeout", type=float, default=6000.0,
                    help="set execution timeout", metavar="SECS")

  parser.add_option("-e", "--esperdir", dest="esperdir", default=default_esper_dir,
                    help="Esper directory", metavar="DIR")

  parser.add_option("-q", "--querydir", dest="querydir", default=default_query_dir,
                    help="query directory", metavar="DIR")

  parser.add_option("-d", "--datadir", dest="datadir",
                    help="data directory", metavar="DIR")

  parser.add_option("-o", "--outdir", dest="outdir",
                    help="output directory", metavar="DIR")

  (options, args) = parser.parse_args()
  
  if len(args) < 1:
    parser.error("no query specified")
  
  query_name = args[0]
  poll_period = options.poll_period
  timeout = options.timeout
  outdir = options.outdir
  
  query_dir = options.querydir
  script_dir = os.getcwd()
  esper_dir = options.esperdir

  # Check query files

  finance = query_name in finance_queries.keys()
  tpch = query_name in tpch_queries.keys()
  if not(finance or tpch) or (finance and tpch):
    print "Invalid query: " + query_name
    print "Valid queries: {0}, {1}".format(str(finance_queries.keys()), str(tpch_queries.keys()))
    exit(1)    

  if query_dir == default_query_dir:
    query_sub_dir = os.path.join(query_dir, ("finance" if finance else "tpch"))
    query_file = os.path.join(query_sub_dir, query_name+".esper")
  else:
    query_file = os.path.join(query_dir, query_name+".esper")

  script_name = 'run_finance.sh' if finance else 'run_tpch.sh'
  script_file = os.path.join(script_dir, script_name)
  
  test_paths = [esper_dir, query_file, outdir, script_file]
  valid_paths = [os.path.exists(p) for p in test_paths]
  if all(valid_paths):
    print "Running query " + query_name
    
    script_args = ' '.join(finance_queries[query_name] if finance else tpch_queries[query_name])
    script = os.path.join(script_dir, script_name)+' '+query_file+' '+script_args
    fscript = format_finance_args(script, options) if finance else format_tpch_args(script, options)
    run_spe(fscript, query_name, poll_period, timeout, outdir)

  else:
    invalid_paths = map(lambda x,y: y, \
                        filter(lambda x,y: not(x), zip(valid_paths, test_paths)))
    print "Invalid paths: " + str(invalid_paths)
    
  