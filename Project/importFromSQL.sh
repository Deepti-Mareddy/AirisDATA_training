#! /bin/bash

# here all the variables inside the config will be available
source /home/hduser/information.config

# remove the existing folder

hdfs dfs -rm -r ${outputpath}

#Sqoop Command

sqoop import-all-tables \
--connect jdbc:mysql://localhost/${databasename} \
--username ${username} \
--password ${password} \
--warehouse-dir ${outputpath} \
-m 2 \
--autoreset-to-one-mapper 

# $? is used to find the return value of the last executed command. 

if [ $? -eq 0 ]
then
echo "Sqoop Statement executed Successfully"
else
echo "Some error occured"
fi
