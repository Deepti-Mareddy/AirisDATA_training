
#! /bin/bas

# here all the variables inside the config will be available
source /home/hduser/information_hive.config


hive <<EOF
create database ${database};
use ${database};
drop table if exists ${table1};
create external table ${table1}(id int,property_type string,room_type string,bathrooms float,bedrooms int,minimum_nights int, location_id int,price int) row format delimited fields terminated by "," stored as textfile;
load data inpath "${path_table1}" into table ${table1};
drop table if exists ${table2};
create external table ${table2}(id int,city string,state string,country string,pincode int)row format delimited fields terminated by "," stored as textfile;
load data inpath "${path_table2}" into table ${table2};
select min(price),max(price),avg(price) from ${table1};
select property_type,count(property_type) from ${table1} group by property_type;
select id,property_type,room_type,price,case when price>avg(price) then 'High Price' else 'Low Price' end from ${table1} group by id,property_type,room_type,price;
select r.location_id,l.city,l.state,l.pincode,count(r.location_id) from ${table1} r join ${table2} l on r.location_id=l.id group by r.location_id,l.city,l.state,l.pincode;
select r.location_id,l.city,min(r.price),max(r.price),avg(r.price) from ${table1} r join ${table2} l on r.location_id=l.id group by r.location_id order by r.location_id;



