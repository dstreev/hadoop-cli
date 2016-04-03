create database if not exists ${DB};

use ${DB};

drop table hdfs_analysis;

create external table if not exists hdfs_analysis (
permissions_long string,
replication int,
user_ string,
group_ string,
size bigint,
block_size bigint,
ratio double,
mod string,
access string,
path_ string,
ip_address string,
hostname string,
block_id string
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${LOCATION}';

drop table hosts;

-- Build a hosts table from known blocks.  Might be better to manually build this
-- record set to ensure ALL hosts are accounted for!!
create table hosts
stored as orc
 as
select distinct hostname from hdfs_analysis;

select * from hosts;