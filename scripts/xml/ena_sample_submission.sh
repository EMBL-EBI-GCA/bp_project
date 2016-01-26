#!/bin/bash
# A script for registering samples in ENA using curl

usage="\n\tUsage: bash ena_sample_submission.sh submission.xml sample.xml dir_path ega_box ega_password\n"

if [ $# -lt 5 ]; then
  echo 1>&2 "$0: not enough arguments"
  echo -e ${usage}
  exit 2
elif [ $# -gt 5 ]; then
  echo 1>&2 "$0: too many arguments"
  echo -e ${usage}
  exit 2
fi

submission=$1
xml=$2
dir=$3
ega_user=$4
ega_pass=$5
url='https://www.ebi.ac.uk/ena/submit/drop-box/submit/?auth=ENA%20'${ega_user}'%20'${ega_pass}


xml=${dir}'/'${xml}
submission=${dir}'/'${submission}

link_xml='@'${xml}
link_sub='@'${submission}

if [[ -f "$xml"  &&  -f "$submission" ]]
then
  count=0
  count=`grep alias ${xml}|wc -l`
  if [ $count -gt 0 ]
  then
    echo $xml ' has ' $count ' records'
    rsp=`curl -s -F "SUBMISSION=${link_sub}"  -F "SAMPLE=${link_xml}" "${url}"`
    if [[ ${rsp} =~ "success=\"true\"" ]]
    then
       echo "True, registered samples in $xml file"
    elif [[ ${rsp} =~ "success=\"false\"" ]]
    then
       echo "False, not registered samples in $xml file"
       echo $rsp
    else
       echo "Something went wrong"
       echo $rsp
    fi 
  else
    echo "No new entry present in $xml file"
  fi
else
  echo "$xml or $submission not accessible"
fi
