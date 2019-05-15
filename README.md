## HADOOP-CLI

HADOOP-CLI is an interactive command line shell that makes interacting with the Hadoop Distribted Filesystem (HDFS)
simpler and more intuitive than the standard command-line tools that come with Hadoop. If you're familiar with OS X, Linux, or even Windows terminal/console-based applications, then you are likely familiar with features such as tab completion, command history, and ANSI formatting.

### Binary Package

[Pre-Built Distribution](https://github.com/dstreev/hadoop-cli/releases)

Download the release files to a temp location.  As a root user, chmod +x the 3 shell script files then run the 'setup.sh'.  This will create and install the hdfscli application to your path.

Try it out on a host with default configs:

    hadoopcli

To use an alternate HADOOP_CONF_DIR:

    hadoopcli --config /var/hadoop/dev-cfg

### Release Notes

#### 2.0.11-SNAPSHOT

A lot of cumulative work has made it into this release.

##### Command Line Enhancements:
- 'silent' option added to suppress output.  Helpful when using the 'run' option.
- 'run' option to 'run' a file with a list of commands.
- 'stdin' option added so commands could be piped into the cli and run under the same session.
- 'debug' and 'verbose' options added to increase visibility of actions being run.

##### Functional Enhancements:
Each function has a help option:
`help <function>`

###### Core HDFS Commands
- `count`
- `test`

###### lsp (ls plus)
- filter support (regex using Java Pattern matching to find matching paths in search)
- formatting options
- output to hdfs option
- control over recursiveness with 'depth' option
- comment option to enhance output.  Nice to be able to carry extra metadata along in a pipeline.


#### 1.0.0-SNAPSHOT (in-progress)

This release is the first for `hadoopcli` and is an extension to (replacement of) `hdfs-cli` found [here](https://github.com/dstreev/hdfs-cli)

Building on release [2.3.2-SNAPSHOT](https://github.com/dstreev/hdfs-cli) of `hdfscli`, this version starts to integrate with the`Job History Server`, `YARN` and `ATS`.  Hence to change from `hdfscli` to `hadoopcli`.


### Building

This project requires the artifacts from https://github.com/dstreev/stemshell , which is a forked enhancement that has added support for processing command line parameters and deals with quoted variables.

Since we're now doing more in the interface and writing results to hdfs, we need to build binary compatible packages.  The default `mvn` profile is for Apache Hadoop 2.7.  There is a profile for Apache Hadoop 2.6.

```
# For 2.7
mvn -DskipTests clean install -P 2.7
```


```
# For 2.6
mvn -DskipTests clean install -P 2.6
```


### Basic Usage
HADOOP-CLI works much like a command-line ftp client: You first establish a connection to a remote HDFS filesystem,
then manage local/remote files and transfers.

To start HADOOP-CLI, run the following command:

	java -jar hadoop-cli-full-bin.jar
		
### Command Documentation

Help for any command can be obtained by executing the `help` command:

	help pwd

Note that currently, documentation may be limited.

#### Local vs. Remote Commands
When working within a HADOOP-CLI session, you manage both local (on your computer) and remote (HDFS) files. By convention, commands that apply to both local and remote filesystems are differentiated by prepending an `l`
character to the name to denote "local".

For example:

`lls` lists local files in the local current working directory.

`ls` lists remote files in the remote current working directory.

Every HADOOP-CLI session keeps track of both the local and remote current working directories.

### Support for External Configurations (core-site.xml,hdfs-site.xml)

By default, hdfs-cli will use `/etc/hadoop/conf` as the default location to search for
`core-site.xml` and `hdfs-site.xml`.  If you want to use an alternate, use the `--config`
option when starting up hdfs-cli.

The `--config` option takes 1 parameter, a local directory.  This directory should contain hdfs-site.xml and core-site.xml files.  When used, you'll automatically be connected to hdfs and changed to you're hdfs home directory.

Example Connection parameters.

    # Use the hadoop files in the input directory to configure and connect to HDFS.
    hadoopcli --config ../mydir

This can be used in conjunction with the 'Startup' Init option below to run a set of commands automatically after the connection is made.  The 'connect' option should NOT be used in the initialization script.

### Startup Initialization Option

Using the option '-i <filename>' when launching the CLI, it will run all the commands in the file.

The file needs to be location in the $HOME/.hadoop-cli directory.  For example:

	# If you're using the helper shell script
	hadoopcli -i test
	
	# If you're using the java command
	java -jar hadoop-cli-full-bin.jar -i test
	

Will initialize the session with the command(s) in $HOME/.hadoop-cli/test. One command per line.

The contents could be any set of valid commands that you would use in the cli. For example:

	cd user/dstreev

### NN Stats

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

### Job History Stats
	
Delivered - Docs to come

### Scheduler Stats

Delivered - Docs to come

### Container Stats

Delivered - Docs to come
	
### Enhanced Directory Listing (lsp)

Like 'ls', you can fetch many details about a file.  But with this, you can also add information about the file that includes:
- Block Size
- Access Time
- Ratio of File Size to Block
- Datanode information for the files blocks (Host and Block Id)

Use help to get the options:
    
    help lsp

```    
usage: stats [OPTION ...] [ARGS ...]
Options:
 -d,--maxDepth <maxDepth>      Depth of Recursion (default 5), use '-1'
                               for unlimited
 -f,--format <output-format>   Comma separated list of one or more:
                               permissions_long,replication,user,group,siz
                               e,block_size,ratio,mod,access,path,datanode
                               _info (default all of the above)
 -o,--output <output>          Output File (HDFS) (default System.out)
```

When not argument is specified, it will use the current directory.

Examples:
    
    # Using the default format, output a listing to the files in `/user/dstreev/perf` to `/tmp/test.out`
    lsp -o /tmp/test.out /user/dstreev/perf

Output with the default format of:

    permissions_long,replication,user,group,size,block_size,ratio,mod,access,path,datanode_info
    
```
   rw-------,3,dstreev,hdfs,429496700,134217728,3.200,2015-10-24 12:26:39.689,2015-10-24 12:23:27.406,/user/dstreev/perf/teragen_27/part-m-00004,10.0.0.166,d2.hdp.local,blk_1073747900
   rw-------,3,dstreev,hdfs,429496700,134217728,3.200,2015-10-24 12:26:39.689,2015-10-24 12:23:27.406,/user/dstreev/perf/teragen_27/part-m-00004,10.0.0.167,d3.hdp.local,blk_1073747900
   rw-------,3,dstreev,hdfs,33,134217728,2.459E-7,2015-10-24 12:27:09.134,2015-10-24 12:27:06.560,/user/dstreev/perf/terasort_27/_partition.lst,10.0.0.166,d2.hdp.local,blk_1073747909
   rw-------,3,dstreev,hdfs,33,134217728,2.459E-7,2015-10-24 12:27:09.134,2015-10-24 12:27:06.560,/user/dstreev/perf/terasort_27/_partition.lst,10.0.0.167,d3.hdp.local,blk_1073747909
   rw-------,1,dstreev,hdfs,543201700,134217728,4.047,2015-10-24 12:29:28.706,2015-10-24 12:29:20.882,/user/dstreev/perf/terasort_27/part-r-00002,10.0.0.167,d3.hdp.local,blk_1073747920
   rw-------,1,dstreev,hdfs,543201700,134217728,4.047,2015-10-24 12:29:28.706,2015-10-24 12:29:20.882,/user/dstreev/perf/terasort_27/part-r-00002,10.0.0.167,d3.hdp.local,blk_1073747921
```

With the file in HDFS, you can build a [hive table](./src/main/hive/lsp.ddl) on top of it to do some analysis.  One of the reasons I created this was to be able to review a directory used by some process and get a baring on the file construction and distribution across the cluster.  

#### Use Cases
- The ratio can be used to identify files that are below the block size (small files).
- With the Datanode information, you can determine if a dataset is hot-spotted on a cluster.  All you need is a full list of hosts to join the results with.

### Available Commands

#### Common Commands
	help		display help information
	put			upload local files to the remote HDFS
    get			(todo) retrieve remote files from HDFS to Local Filesystem

#### Remote (HDFS) Commands
	cd		 change current working directory
	ls		 list directory contents
	rm		 delete files/directories
	pwd		 print working directory path
	cat		 print file contents
	chown	 change ownership
	chmod	 change permissions
	chgrp	 change group
	head	 print first few lines of a file
	mkdir	 create directories
	count    Count the number of directories, files and bytes under the paths that match the specified file pattern.
	stat     Print statistics about the file/directory at <path> in the specified format.
	tail     Displays last kilobyte of the file to stdout.
	text	 Takes a source file and outputs the file in text format.
	touchz Create a file of zero length.
	usage	 Return the help for an individual command.

	createSnapshot	Create Snapshot
	deleteSnapshot	Delete Snapshot
	renameSnapshot	Rename Snapshot

#### Local (Local File System) Commands
	lcd		 change current working directory
	lls		 list directory contents
	lrm		 delete files/directories
	lpwd	 print working directory path
	lcat	 print file contents
	lhead	 print first few lines of a file
	lmkdir	 create directories

#### Tools and Utilities
    lsp     ls plus.  Includes Block information and locations.     
    nnstat  Namenode Statistics

### Known Bugs/Limitations

* No support for paths containing spaces
* No support for Windows XP
* Path Completion for chown, chmod, chgrp, rm is broken

### Road Map

- Support input variables
- Expand to support Extended ACL's (get/set)
- Add Support for setrep
- HA Commands
	- NN and RM
	




