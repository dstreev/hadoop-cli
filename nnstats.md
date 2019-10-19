# NN Stats

Collect Namenode stats from the available Namenode JMX url's.

3 Type of stats are current collected and written to hdfs (with -o option) or to screen (no option specified)

The 'default' delimiter for all records is '\u0001' (Cntl-A)

>> Namenode Information: (optionally written to the directory 'nn_info')
  Fields: Timestamp, HostAndPort, State, Version, Used, Free, Safemode, TotalBlocks, TotalFiles, NumberOfMissingBlocks, NumberOfMissingBlocksWithReplicationFactorOne

>> Filesystem State: (optionally written to the directory 'fs_state')
  Fields: Timestamp, HostAndPort, State, CapacityUsed, CapacityRemaining, BlocksTotal, PendingReplicationBlocks, UnderReplicatedBlocks, ScheduledReplicationBlocks, PendingDeletionBlocks, FSState, NumLiveDataNodes, NumDeadDataNodes, NumDecomLiveDataNodes, NumDecomDeadDataNodes, VolumeFailuresTotal

>> Top User Operations: (optionally written to the directory 'top_user_ops')
  Fields: Timestamp, HostAndPort, State, WindowLenMs, Operation, User, Count

[Hive Table DDL for NN Stats](./src/main/hive/nn_stats.ddl)
