#!/bin/bash
#****************************************************************
#Script Name    : csv_upload.sh
#Description    : This script will generate the csv file that will be loaded to BQ tables.
#Created by     : vibhor
#Version        Author          Created Date    Comments
#1.0            Vibhor          2020-08-20      Modify Incremenatal logic change coll. 
#****************************************************************


#------Global variable--------------#
NO_OF_PARAMETERS=3

#-----------------------------------#
#----- Check for the number of input parameters-------#

if [ $# -ne ${NO_OF_PARAMETERS} ]
then
        echo "Error: The script expects ${NO_OF_PARAMETERS} parameters but the actual parameters that are passed are $#"
        echo "Listed below are the list of parameters required by the script"
        echo "1- Enter Source Name - ARADMIN"
		echo "2- Enter Table Name"
		echo "3- Enter Sleep Time"
        exit 1
fi


 #export CLOUDSDK_PYTHON=/usr/lib/python2.7/python
 #unset ORACLE_HOME
 #export GOOGLE_APPLICATION_CREDENTIALS=/home/sysbosadmin/aiboski/bigquery/jsonref/credential.json
 export JAVA_HOME=/home/sysbosadmin/java/jdk1.8.0_241
 export PATH=$JAVA_HOME/bin:$PATH
 sqlcl="/home/sysbosadmin/sqlcl/bin/sql"
 projectid=PROJ
 dataset=ARADMIN_PROD
 csv_data_path=/home/sysbosadmin/aiboski/bigquery/csvdata
 bkup_dir=/home/sysbosadmin/aiboski/bigquery/archives
 log_dir=/home/sysbosadmin/aiboski/bigquery/logs
 sql_dir=/home/sysbosadmin/aiboski/bigqery/sqls
 schema_dir=/home/sysbosadmin/aiboski/bigquery/schemas
 rundate=`date +%Y%m%d%H%M`
 log_file="$rundate-$1_$2.log"
 log_path="${log_dir}/${log_file}"

 csvgen()
  {
   if
    [ "$#" -ne 2 ]; then
    echo "Check the Input Parameter => OWNER - TABLE NAME" >> $log_path
    exit 1
   fi

   OWNER_TABLENAME="$1"
   ORACLE_TABLENAME="$2"
   echo "Generating csv for the $OWNER_TABLENAME.$ORACLE_TABLENAME" >> $log_path
   $sqlcl -s ARABOSADMIN/REMO123@//itbosski-ax0555.comp.com:1521/LOGICAL_IT0777.Comp.COM  << EOF
   whenever sqlerror exit sql.sqlcode;
   set echo off;
   set feedback off;
   set trimspool on;
   set termout off;
   set showmode off;
   set pagesize 0;
   set serverout off;
   column  max_rowscn_tracker new_value max_rowscn_tracker;
   column  max_rowscn_source new_value max_rowscn_source;
   select max(art_rowscn) max_rowscn_tracker from aiboski_rowscn_tracker where art_tableowner='$OWNER_TABLENAME' and art_tablename='$ORACLE_TABLENAME';
   select max(C3) max_rowscn_source from $OWNER_TABLENAME.$ORACLE_TABLENAME;
   spool $csv_data_path/${ORACLE_TABLENAME}_TABLE_DATA_$rundate.csv;
   select /*csv*/ * from $OWNER_TABLENAME.$ORACLE_TABLENAME where C3 > &max_rowscn_tracker and C3 <= &max_rowscn_source;
   spool off; 
   insert into  aiboski_rowscn_tracker (ART_TABLEOWNER, ART_TABLENAME, ART_ROWSCN, ART_CREATED_BY, ART_CREATED_DT, ART_UPDATED_BY, ART_UPDATED_DT) select '$OWNER_TABLENAME','$ORACLE_TABLENAME','&max_rowscn_source',user,sysdate,user,sysdate from dual where not exists (select  * from aiboski_rowscn_tracker where ART_ROWSCN ='&max_rowscn_source' and ART_TABLENAME = '$ORACLE_TABLENAME'); 
   commit;
   exit;
EOF
}

bqupload()
  {
  BQ_OWNER_TABLENAME="$1"
  TABLENAME="$2"
  exec >> $log_path  2>&1
   for tablefilecsv in `ls -r $csv_data_path/$BQ_ORACLE_TABLENAME* | sed -r 's/^.+\///'`; do
   FILENAME=$tablefilecsv
   #TABLENAME=${FILENAME%_TABLE*}

   echo "  Filename: (${FILENAME})"
   echo "  Tablename: (${TABLENAME})"
 
   echo "Numbers of records before Upload in Table :${TABLENAME}"
   bq query 'SELECT COUNT(*) FROM '${dataset}.${TABLENAME}
 
   echo "Uploading Data in Bigquery"
   bq --nosync load --source_format=CSV --skip_leading_rows=3 --allow_jagged_rows=TRUE --max_bad_records=10000 \
      --allow_quoted_newlines=TRUE  $projectid:$dataset.$TABLENAME \
       $csv_data_path/$FILENAME $schema_dir/$TABLENAME.json
   status=$?

   if 
   [ $status -ne 0 ]; then 
     echo "Check logs"
     exit 1
   else
   sleep $3   
   echo "Numbers of records after Upload  in Table :${TABLENAME}"
   bq query 'SELECT COUNT(*) FROM '${dataset}.${TABLENAME}
 
   echo "Zipping and Archiving file "
   gzip  $csv_data_path/$FILENAME
   mv $csv_data_path/$FILENAME.gz  $bkup_dir
  fi
  done
 }

uploadmail()
  {
   mailist='group@abcd.com'
   rundate=`date +%Y%m%d`
   mailmsg=" [Bigquery Data Upload] -${projectid}:${dataset}-${rundate} job Run"

  errlog=`grep "Error" $log_path | wc -l`

  if  [ $errlog -ne 0 ];
   then
   echo " $0: Data is not exported successfully ... check errors in $log_path" 
  #--------------------
  # Mail Broadcast
  #------------------
  mail -s "$mailmsg Error Notification `date '+%d-%m-%Y %H:%M'` " $mailist < $log_path
  else 
   echo " $0: Data is exported successfully ... check logs in $log_path" 
  #--------------------
  # Mail Broadcast
  #------------------
  mail -s "$mailmsg Jobs Notification `date '+%d-%m-%Y %H:%M'` " $mailist < $log_path
 fi
}


###########main#############
csvgen $1 $2
bqupload $1 $2 $3 
uploadmail
