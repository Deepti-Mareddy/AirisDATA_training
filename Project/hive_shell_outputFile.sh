
#! /bin/bash

# here all the variables inside the config will be available
source /home/hduser/information_hive.config

hive -e "create database ${database}"
hive -e "use ${database}"
hive -e "drop table if exists ${table1}"
hive -e "create external table ${table1}(id int,property_type string,room_type string,bathrooms float,bedrooms int,minimum_nights int, location_id int,price int) row format delimited fields terminated by ',' stored as textfile"
hive -e "load data inpath '${path_table1}' into table ${table1}"
hive -e "drop table if exists ${table2}"
hive -e "create external table ${table2}(id int,city string,state string,country string,pincode int)row format delimited fields terminated by ',' stored as textfile"
hive -e "load data inpath '${path_table2}' into table ${table2}"
hive -e "select min(price),max(price),avg(price) from ${table1}" >> "./QueryOutput1"
hive -e "select property_type,count(property_type) from ${table1} group by property_type" >> "./QueryOutput2"
hive -e "select id,property_type,room_type,price,case when price>avg(price) then 'High Price' else 'Low Price' end from ${table1} group by id,property_type,room_type,price" >> "./QueryOutput3"
hive -e "select r.location_id,l.city,l.state,l.pincode,count(r.location_id) from ${table1} r join ${table2} l on r.location_id=l.id group by r.location_id,l.city,l.state,l.pincode" >> "./QueryOutput4"
hive -e "select r.location_id,l.city,min(r.price),max(r.price),avg(r.price) from ${table1} r join ${table2} l on r.location_id=l.id group by r.location_id,l.city" >> "./QueryOutput5"



