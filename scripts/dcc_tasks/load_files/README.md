# Scripts to load incoming files in our system
We do not expect this code to be generally useful but if you have any questions about the code you find here or our processes managing the Blueprint data please email [blueprint-info@ebi.ac.uk](mailto:blueprint-info@ebi.ac.uk)
## check_md5.sh 
Run like:
```
bsub -o file_md5.%J.%I -J file_md5[1-3]%3 sh check_md5.sh files_to_check.txt
```
Where ```[1-3]``` controls the number of jobs to submit to the cluster and ```%3``` controls the number of concocurrent jobs
File named ```files_to_check.txt``` containing the files to check
This file needs to have the following format:
 ```
 004g2009c4396dbcc3bbf72ee7915649  exampleFile.bam
  ```
  Where there must be exactly 2 spaces between the md5 sum and the file name
