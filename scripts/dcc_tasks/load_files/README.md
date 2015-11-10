##Scripts to load incoming files in our system

check_md5.sh is run like: bsub -J file_md5[1-3]%3 sh check_md5.sh
Where the '[1-3]' controls the number of jobs to submit to the cluster and '%3' controls the number of concocurrent jobs