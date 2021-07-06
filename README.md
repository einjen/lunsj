# lunsj
lustre jobstats comparison

this tries to summarize all lustre jobstats from compute ndoes per slurm job per user, and additionally summarizes stats from certain specific nodes (login, service, transfernodes etc)

it does NOT (yet) collect stats that originate from users on computenodes that are NOT marked by slurmjob.

also, there is a prerequisite that the naming of the computenodes starts with letter "b" (rootnodefilter)



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

  I sincerely apologize for poorly written python, it was done in an afternoon,  but this works for me. It is what it is
