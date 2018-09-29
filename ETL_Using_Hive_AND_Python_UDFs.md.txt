# Prmary Table creation:  
## Creation of a Table in Hive:  
```SQL
CREATE TABLE pageviews (userid STRING COMMENT 'A sample comment on a column', 
link STRING, 
came_from STRING) 
COMMENT 'This is the a sample table for ETL'
PARTITIONED BY (datestamp STRING) 
CLUSTERED BY (userid) INTO 10 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;
```  
  
## Insertion of values using static partition:  
```SQL
INSERT INTO TABLE pageviews PARTITION (datestamp = '2014-09-23')
  VALUES ('jsmith', 'mail.com', 'sports.com'), ('jdoe', 'mail.com', null);
  ```  
    
## Insertion of values using Dynamic partition:  
```SQL
set hive.exec.dynamic.partition=true
  
  
set hive.exec.dynamic.partition.mode=nonstrict;


INSERT INTO TABLE pageviews PARTITION (datestamp)
  VALUES ('tjohnson', 'sports.com', 'finance.com', '2014-09-23'), ('tlee', 'finance.com', null, '2014-09-21');
  ```  
  
## Testing:  
```SQL
select * from pageviews where came_from is NULL; 
```   
  
# External Staging table creation:  
  
## Input Preparations:  
```SQL
cat /tmp/part1
xxx,xx.com,,2018-09-10
yyy,yyy.com,sports.com,2018-09-10
jdoe,mail.com,,2018-09-10
tjohnson,sports.com,finance.com,2018-09-10

cat /tmp/part2
xxx,xx.com,,2018-09-10
yyy,yyy.com,sports.com,2018-09-10
jdoe,mail.com,,2018-09-10
tjohnson,sports.com,finance.com,2018-09-10

hadoop fs -mkdir /user/cloudera/hivetabledir

hadoop fs -copyFromLocal /tmp/part* /user/cloudera/hivetabledir

hadoop fs -ls /user/cloudera/hivetabledir

hadoop fs -cat /user/cloudera/hivetabledir/part*
xxx,xx.com,,2018-09-10
yyy,yyy.com,sports.com,2018-09-10
jdoe,mail.com,,2018-09-10
tjohnson,sports.com,finance.com,2018-09-10
xxx,xx.com,,2018-09-10
yyy,yyy.com,sports.com,2018-09-10
jdoe,mail.com,,2018-09-10
tjohnson,sports.com,finance.com,2018-09-10
```
  
## External Table Creation from data at HDFS location:  
```SQL
CREATE EXTERNAL TABLE pageviewstaging(userid STRING, 
link STRING, 
came_from STRING,
datestamp STRING)
COMMENT 'This is the staging table'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/cloudera/hivetabledir';
```
  
## Testing:  
```SQL
select * from pageviewstaging;
```
  
# Partitioned External Table  
## Input Preparations for Partitioned External Table:  
```SQL
[cloudera@quickstart ~]$ cat /tmp/partitionedpart1
1,1,1
2,2,2
3,3,3
4,4,4
5,5,5
[cloudera@quickstart ~]$ cat /tmp/partitionedpart2
6,6,6
7,7,7
8,8,8
9,9,9
10,10,10
[cloudera@quickstart ~]$ hadoop fs -mkdir /user/cloudera/hiveparttabledir
[cloudera@quickstart ~]$ hadoop fs -mkdir /user/cloudera/hiveparttabledir/date=2010-02-22
[cloudera@quickstart ~]$ hadoop fs -mkdir /user/cloudera/hiveparttabledir/date=2011-02-22
[cloudera@quickstart ~]$ hadoop fs -copyFromLocal /tmp/partitionedpart1 /user/cloudera/hiveparttabledir/date=2010-02-22
[cloudera@quickstart ~]$ hadoop fs -copyFromLocal /tmp/partitionedpart2 /user/cloudera/hiveparttabledir/date=2011-02-22
```
  

## External Partitioned Table Creation from data at HDFS location:  
```SQL
CREATE EXTERNAL TABLE user (
  userId INT,
  type INT,
  level INT
)
COMMENT 'User Infomation'
PARTITIONED BY (date String)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/cloudera/hiveparttabledir/';
```
  
## Neccessary Partitions repairs:  
```SQL
MSCK REPAIR TABLE user;
```

## Testing:  
```SQL
select * from user;
```
   
# Staging data to Primary Table  
  
## Loading data into Primary Partitioned Table from external staging table:  
```SQL
insert into table pageviews partition(datestamp)
select *
from 
pageviewstaging;
```  
  
# UDF's in Hive  
  
## Prepare Python Script  
```SQL
[cloudera@quickstart ~]$ cat /tmp/udf.py
import sys
import datetime
for line in sys.stdin:
  line = line.strip()
  fname , lname = line.split('\t')
  l_name = lname.lower()
  print '::'.join([fname, str(l_name)])
```
  
## Prepare Input Data  
```SQL
[cloudera@quickstart ~]$ cat /tmp/mytableData
x	x
y	y
z	z
```
  
## Copy input data to HDFS  
```SQL
[cloudera@quickstart ~]$ hadoop fs -copyFromLocal /tmp/mytableData /user/cloudera/hivetabledir/
```
  
## Create table in Hive  
```SQL
hive>>CREATE table IF NOT EXISTS mytable(
fname STRING,
lname STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
```
  
## Load data into Hive table using Hive  
```SQL
hive>>LOAD DATA INPATH '/user/cloudera/hivetabledir/mytableData' OVERWRITE INTO TABLE mytable;
```
  
## add python file into Hive  
```SQL
hive>>add FILE /tmp/udf.py;
```
  
## Apply UDF's on table into Hive  
```SQL
hive>>SELECT TRANSFORM(fname, lname) USING 'python udf.py' AS (fname, l_name) FROM mytable;
```
