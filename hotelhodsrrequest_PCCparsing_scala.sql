create table IF NOT EXISTS hotelhodsrrequest_PCCparsing_scala (PCC String, PropertyCode String, Year String, Month String, Duplication_Counts int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INPATH '/user/sg952655/total.gz' overwrite INTO TABLE hotelhodsrrequest_PCCparsing_scala; -- not LOCAL INPATH because the data is already in HDFS:: overwrite to overwite the existing Data


--Compressed Data Storage
--Keeping data compressed in Hive tables has, in some cases, been known to give better performance than uncompressed storage; both in terms of disk usage and query performance.
--You can import text files compressed with Gzip or Bzip2 directly into a table stored as TextFile. The compression will be detected automatically and the file will be decompressed on-the-fly during query execution. For example:
CREATE TABLE raw (line STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '/tmp/weblogs/20090603-access.log.gz' INTO TABLE raw;
--The table 'raw' is stored as a TextFile, which is the default storage. However, in this case Hadoop will not be able to split your file into chunks/blocks and run multiple maps in parallel. This can cause underutilization of your cluster's 'mapping' power.
--The recommended practice is to insert data into another table, which is stored as a SequenceFile. A SequenceFile can be split by Hadoop and distributed across map jobs whereas a GZIP file cannot be. For example:
CREATE TABLE raw (line STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';
 
CREATE TABLE raw_sequence (line STRING)
STORED AS SEQUENCEFILE;
 
LOAD DATA LOCAL INPATH '/tmp/weblogs/20090603-access.log.gz' INTO TABLE raw;
 
SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK; -- NONE/RECORD/BLOCK (see below)
INSERT OVERWRITE TABLE raw_sequence SELECT * FROM raw;
--The value for io.seqfile.compression.type determines how the compression is performed. Record compresses each value individually while BLOCK buffers up 1MB (default) before doing compression.


--Writing into SequenceFile Formate table.

set mapreduce.map.memory.mb=12288;
set mapreduce.map.java.opts= -Xmx8192M -verbose:gc -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70;
set mapreduce.reduce.memory.mb=12288;
set mapreduce.reduce.java.opts= -Xmx8192M -verbose:gc -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70;
--CMSInitiatingOccupancyFraction=70 means the server starts collecting when 70% of tenured gen is full to avoid forced collection -XX:+UseCMSInitiatingOccupancyOnly Do not attempt to adjust CMS setting • You should experience average GC times between 50ms - 150ms • Considering hardware with heaps of 32GB or less • If your application maintains a large cache of long-lived objects in the heap, consider increasing the threshold triggering setting: -XX:CMSInitiatingOccupancyFraction=90
--Specifically, a good rule of thumb is to set the jave heap size to be 10% less than the container size => mapreduce.{map|reduce}.java.opts = mapreduce.{map|reduce}.memory.mb x 0.9
--If a YARN container grows beyond its heap size setting, the map or reduce task will fail. The default heapsize for mappers is 1.5GB and for reducers is 2.5GB .
--To execute the actual map or reduce task, YARN will run a JVM within the container. The Hadoop property mapreduce.{map|reduce}.java.opts is intended to pass options to this JVM. This can include ­Xmx to set max heap size of the JVM. However, the subsequent growth in the memory footprint of the JVM due to the settings in mapreduce.{map|reduce}.java.opts is limited by the actual size of the container as set by mapreduce.{map|reduce}.memory.mb.


drop table hotelhodsrrequest_PCCparsing_scala_sequence;
create table IF NOT EXISTS hotelhodsrrequest_PCCparsing_scala_sequence (PCC String, PropertyCode String, Year_Partioned String, Month_Partioned String, Duplication_Counts int)
STORED AS SEQUENCEFILE;

SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;

INSERT OVERWRITE TABLE hotelhodsrrequest_PCCparsing_scala_sequence
SELECT hotelhodsrrequest_pccparsing_scala.pcc as PCC,
hotelhodsrrequest_pccparsing_scala.propertycode as PropertyCode,
hotelhodsrrequest_pccparsing_scala.year as Year,
hotelhodsrrequest_pccparsing_scala.month as Month,
hotelhodsrrequest_pccparsing_scala.duplication_counts as Duplication_Counts
FROM hotelhodsrrequest_PCCparsing_scala;

--Partitioning and bucketing of the TABLE

create EXTERNAL table IF NOT EXISTS hotelhodsrrequest_PCCparsing_scala (PCC String, PropertyCode String, Year String, Month String, Duplication_Counts int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n';

LOAD DATA INPATH '/user/sg952655/total.txt' overwrite INTO TABLE hotelhodsrrequest_PCCparsing_scala; 

drop table hotelhodsrrequest_PCCparsing_scala_Partitioned_and_Bucketed;
set mapreduce.map.memory.mb=12288;
set mapreduce.map.java.opts= -Xmx8192M -verbose:gc -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70;
set mapreduce.reduce.memory.mb=12288;
set mapreduce.reduce.java.opts= -Xmx8192M -verbose:gc -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70;

create table IF NOT EXISTS hotelhodsrrequest_PCCparsing_scala_Partitioned_and_Bucketed (PCC String, PropertyCode String, Duplication_Counts int)
partitioned by (Year String, Month String)
CLUSTERED BY (PCC) INTO 20 BUCKETS;
--(would create to many files)

SET hive.exec.max.dynamic.partitions=120000;
SET hive.exec.max.dynamic.partitions.pernode=120000;
SET hive.exec.max.created.files=655350;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.enforce.bucketing = true;
-- (Note: Not needed in Hive 2.x onward)

INSERT OVERWRITE TABLE hotelhodsrrequest_PCCparsing_scala_Partitioned_and_Bucketed partition (Year, Month)
SELECT hotelhodsrrequest_pccparsing_scala.pcc as PCC,
hotelhodsrrequest_pccparsing_scala.propertycode as PropertyCode,
hotelhodsrrequest_pccparsing_scala.duplication_counts as Duplication_Counts,
hotelhodsrrequest_pccparsing_scala.year as Year,
hotelhodsrrequest_pccparsing_scala.month as Month
FROM hotelhodsrrequest_PCCparsing_scala;
--if n is the last position be sure to have the partitioned columens primary in n-1 position and secondary in nth postion
--can also be stroed as a SEQUENCEFILE and also using SEQUENCEFILE as both source and destination.

--Partitioning data is often used for distributing load horizontally, this has performance benefit, and helps in organizing data in a logical fashion. Example: if we are dealing with a large employee table and often run queries with WHERE clauses that restrict the results to a particular country or department . For a faster query response Hive table can be PARTITIONED BY (country STRING, DEPT STRING). Partitioning tables changes how Hive structures the data storage and Hive will now create subdirectories reflecting the partitioning structure like

--.../employees/country=ABC/DEPT=XYZ.
--If query limits for employee from country=ABC, it will only scan the contents of one directory country=ABC. This can dramatically improve query performance, but only if the partitioning scheme reflects common filtering. Partitioning feature is very useful in Hive, however, a design that creates too many partitions may optimize some queries, but be detrimental for other important queries. Other drawback is having too many partitions is the large number of Hadoop files and directories that are created unnecessarily and overhead to NameNode since it must keep all metadata for the file system in memory.

--Bucketing is another technique for decomposing data sets into more manageable parts. For example, suppose a table using date as the top-level partition and employee_id as the second-level partition leads to too many small partitions. Instead, if we bucket the employee table and use employee_id as the bucketing column, the value of this column will be hashed by a user-defined number into buckets. Records with the same employee_id will always be stored in the same bucket. Assuming the number of employee_id is much greater than the number of buckets, each bucket will have many employee_id. While creating table you can specify like CLUSTERED BY (employee_id) INTO XX  BUCKETS; where XX is the number of buckets . Bucketing has several advantages. The number of buckets is fixed so it does not fluctuate with data. If two tables are bucketed by employee_id, Hive can create a logically correct sampling. Bucketing also aids in doing efficient map-side joins etc.

--creating a PARQUET FILE table from the present text file.

create table IF NOT EXISTS hotelhodsrrequest_PCCparsing_scala_parquet LIKE hotelhodsrrequest_pccparsing_scala STORED AS PARQUETFILE;
insert overwrite table hotelhodsrrequest_PCCparsing_scala_parquet select * from hotelhodsrrequest_pccparsing_scala;

--create avro file table from the present text file.
set hive.exec.compress.output=true;
set avro.output.codec=snappy;-- to compress the table

CREATE TABLE if not exists hotelhodsrrequest_pccparsing_scala_avro
(pcc string, propertycode string, year string, month string, duplication_counts int)
STORED AS AVRO
TBLPROPERTIES ('avro.schema.literal'='{
               "name": "hotelhodsrrequest_pccparsing_scala",
               "type": "record",-- type should be record
               "fields": [
               {"name":"pcc", "type":"string"},
               {"name":"propertycode", "type":"string"},
               {"name":"year", "type":"string"},
               {"name":"month", "type":"string"},
               {"name":"duplication_counts", "type":"int"}]}');

INSERT OVERWRITE TABLE hotelhodsrrequest_pccparsing_scala_avro select * from hotelhodsrrequest_pccparsing_scala;

--create RC file table from the present text file.

create table IF NOT EXISTS hotelhodsrrequest_PCCparsing_scala_rcfile like hotelhodsrrequest_pccparsing_scala stored as rcfile;

INSERT OVERWRITE TABLE hotelhodsrrequest_pccparsing_scala_rcfile select * from hotelhodsrrequest_pccparsing_scala;


-- Location in Hive table storage
Location
hadfs://bdaolp012 -ns/user/sg952655/table_name

               
 
