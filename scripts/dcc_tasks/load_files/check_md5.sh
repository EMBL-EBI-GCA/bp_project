#!/bin/bash
#bsub -J file_md5[1-#total_files_to_md5]%1 -o file_md5.%J.%I files.txt
FILE=$1
sed -n ${LSB_JOBINDEX}p ${FILE}| md5sum -c