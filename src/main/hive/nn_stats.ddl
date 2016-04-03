use ${DB};

drop table if exists nn_info;
create external table if not exists nn_info (
date_ String,
HostAndPort String,
State_ String,
Version String,
Used DOUBLE,
Free_ DOUBLE,
Safemode STRING,
TotalBlocks BIGINT,
TotalFiles BIGINT,
NumberOfMissingBlocks BIGINT,
NumberOfMissingBlocksWithReplicationFactorOne BIGINT
)
STORED AS TEXTFILE
LOCATION '${BASE_DIR}/nn_info';

drop table if exists fs_state;
create external table if not exists fs_state (
date_ String,
HostAndPort String,
State_ String,
CapacityUsed BIGINT,
CapacityRemaining BIGINT,
BlocksTotal BIGINT,
PendingReplicationBlocks BIGINT,
UnderReplicatedBlocks BIGINT,
ScheduledReplicationBlocks BIGINT,
PendingDeletionBlocks BIGINT,
FSState String,
NumLiveDataNodes INT,
NumDeadDataNodes INT,
NumDecomLiveDataNodes INT,
NumDecomDeadDataNodes INT,
VolumeFailuresTotal INT
)
STORED AS TEXTFILE
LOCATION '${BASE_DIR}/fs_state';

drop table if exists top_user_ops;
create external table if not exists top_user_ops (
date_ String,
HostAndPort String,
State_ String,
WindowLenMs BIGINT,
Operation String,
User_ String,
Count_ BIGINT)
STORED AS TEXTFILE
LOCATION '${BASE_DIR}/top_user_ops';