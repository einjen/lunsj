!/usr/bin/env python

"""reads a lustre jobstatsfile from all oss"""

__author__      = "Einar Nass Jensen"
__copyright__   = "c"


import yaml
import re
import pprint
import operator

import os.path
from os import path

import sys
import logging


def read_yaml(newdatafile):
    """ A function to read YAML file"""
    with open(newdatafile) as f:
        config = yaml.safe_load(f)

    return config



def main():
"""
  this script will need to be called with an optional argument, defining the path to the ${datadir} data directory where jobstats and slurm squeue output is

  Relies on having aquired jobstats from lustre with:
  (optional) : 
    clush -N -g oss "lctl set_param obdfilter.*.job_stats=clear"
  (mandatory):     
    clush -N -g oss "cat /proc/fs/lustre/obdfilter/*/job_stats" > ${datadir}/jobstats
    sed -E 's/([[:alpha:]]):([[:digit:]])/\1: \2/g' -i ${datadir}/jobstats
    sed '/get_info\|set_info\|quotactl\|getattr\|setattr\|punch\|sync\|create\|destroy/d' -i ${datadir}/jobstats 

  and relies on having slurm squeue from:
  (mandatory): 
    squeue -o '%F  %U %F:%U %u %C %c %D  %N' -t R -h |sort -n > ${datadir}/mslurm5-squeue

  I sincerely apologize for poorly written python, but this works for me. It is what it is


"""


# config file and config layout


  configfile = os.path.join(sys.path[0], 'lunsj.config')


  config = [
    {
      'settings': {
        'basedir' : 'data',
        'dir' : 'xxxxxxxx-xxxx',
        'datafile' : 'jobstats',
        'newdatafile' : 'newjobstats',
        'servicetatsfile' : 'servicejobstats',
        'slurmfile': 'mslurm5-squeue',
        'servicesummary': 'servicenodes-summary',
        'jobsummary': 'jobs-summary',
        'servicenodes' : ['login-1','login-2','preproc','robinhood','login-3']
        }
    }
  ]

###
###    Read config file

  print configfile
  if not path.isfile(configfile):
    print('no configfile')
    with open(configfile, 'w') as yamlfile:
      data = yaml.dump(config, yamlfile)
      print("Wrote configfile")
  else:
    try:
      with open(configfile, "r") as yamlfile:
        data = yaml.safe_load(yamlfile)
        print("Read config successful")
    except:
      print('could not read yaml')
      exit(1)




  if len(sys.argv)-1 > 0:
    datadir=sys.argv[1]
  else:

    datadir = '/root/einjen/data/20210704-2055/'

  if not path.isdir(datadir):
    print('no valid datadirectory')
    sys.exit()


  datafile = os.path.join(datadir,data[0]['settings']['datafile'])
  newdatafile = os.path.join(datadir,data[0]['settings']['newdatafile'])
  servicedatafile = os.path.join(datadir,data[0]['settings']['servicedatafile'])
  slurmfile = os.path.join(datadir,data[0]['settings']['slurmfile'])
  servicesummary = os.path.join(datadir,data[0]['settings']['servicesummary'])
  jobsummary = os.path.join(datadir,data[0]['settings']['jobsummary'])

  servicenodes =  data[0]['settings']['servicenodes']


  a_logger = logging.getLogger()
  a_logger.setLevel(logging.DEBUG)

  output_file_handler = logging.FileHandler(jobsummary)
  stdout_handler = logging.StreamHandler(sys.stdout)

  a_logger.addHandler(output_file_handler)
  a_logger.addHandler(stdout_handler)


  ################
  ###  CONFIG DONE
  
  
  
  with open(datafile) as fh:
    jobdata = fh.read()


  recs = [x for x in re.split('\s-\s', jobdata)]
  servicerecs = [s for s in recs if any(i in s for i in servicenodes)]
  #print(servicerecs)
  kept = [r for r in recs if '0:b' not in r]
  with open(newdatafile, 'w') as fh2:
    fh2.write('\n- '.join(kept))

  with open(servicedatafile, 'w') as fh3:
    fh3.write('job_stats:\n- '+'\n- '.join(servicerecs))

  with open(slurmfile) as fh:
    slurmdata = fh.readlines()

  userjoblist = []
  for i in slurmdata:
    userjob= i.split()[2]
    userjoblist.append(i.split()[2])


# jobs computenodes

  joblist = read_yaml(newdatafile)

  datalist = {}
  for i in userjoblist:
    datalist[i] = [0,0,0,0,0]



  jobs=joblist['job_stats']
  for job in jobs:
    for userjob in userjoblist:
      if str(userjob) in str(job['job_id']):
        newreadsample = 0
        newreadsum =0
        newwritesample = 0
        newwritesum = 0
        newsampletotal = 0
        if job.has_key('read_bytes'):
          newreadsample = datalist[userjob][0]+job['read_bytes']['samples']
          newreadsum = datalist[userjob][0]+job['read_bytes']['sum']
        if job.has_key('write_bytes'):
          newwritesample = datalist[userjob][0]+job['write_bytes']['samples']
          newwritesum = datalist[userjob][0]+job['write_bytes']['sum']
        newsampletotal = newwritesample+newreadsample
        datalist[userjob] = [newreadsample,newreadsum,newwritesample,newwritesum,newsampletotal]

  a_logger.debug('{:20} {:>15} {:>15} {:>15} {:>15}'.format('JOB_ID:USR_ID', 'read samples','write samples','samples total','read bytes','write bytes'))
  a_logger.debug('------------------------------------------------------------------------------------------------------------------')
  for key,value in sorted(datalist.items(), key=lambda e: e[1][2],reverse=True):
    a_logger.debug('{:20} {:15} {:15} {:15} {:15} {:15}'.format(key, value[0],value[2],value[4],value[1],value[3]))

# servicenodes

  joblist = read_yaml(servicedatafile)

  datalist = {}



  jobs=joblist['job_stats']
  
  for job in jobs:
    if job['job_id'] not in datalist:
      datalist[job['job_id']] = [0,0,0,0,0]

    newreadsample = 0
    newreadsum =0
    newwritesample = 0
    newwritesum = 0
    newsampletotal = 0

    if job.has_key('read_bytes'):
      newreadsample = datalist[job['job_id']][0]+job['read_bytes']['samples']
      newreadsum = datalist[job['job_id']][0]+job['read_bytes']['sum']
    if job.has_key('write_bytes'):
      newwritesample = datalist[job['job_id']][0]+job['write_bytes']['samples']
      newwritesum = datalist[job['job_id']][0]+job['write_bytes']['sum']
    newsampletotal = newwritesample+newreadsample
    datalist[job['job_id']] = [newreadsample,newreadsum,newwritesample,newwritesum,newsampletotal]

  a_logger.debug("")
  a_logger.debug("Servicenodes Summary")
  a_logger.debug('{:20} {:>15} {:>15} {:>15} {:>15}'.format('USR_ID:  NODE', 'read samples','write samples','samples total','read bytes','write bytes'))
  a_logger.debug('------------------------------------------------------------------------------------------------------------------')
  for key,value in sorted(datalist.items(), key=lambda e: e[1][4],reverse=True):
    a_logger.debug('{:10} {:10} {:15} {:15} {:15} {:15} {:15}'.format(key.split(':')[1], key.split(':')[2].split('.')[0],value[0],value[2],value[4],value[1],value[3]))




if __name__== "__main__":
        main()



