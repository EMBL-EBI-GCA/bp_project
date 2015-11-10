#!/bin/bash
#BSUB -J file_md5[1-#total_files_to_md5]%1
#BSUB -o file_md5.%J.%I
FILE=$1
sed -n ${LSB_JOBINDEX}p ${FILE}| md5sum -c