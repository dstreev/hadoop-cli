## HADOOP-CLI

HADOOP-CLI is an interactive command line shell that makes interacting with the Hadoop Distribted Filesystem (HDFS)
simpler and more intuitive than the standard command-line tools that come with Hadoop. If you're familiar with OS X, Linux, or even Windows terminal/console-based applications, then you are likely familiar with features such as tab completion, command history, and ANSI formatting.

### Binary Package

[Pre-Built Distribution](https://github.com/dstreev/hadoop-cli/releases)

1. Download the release 'tar.gz' file to a temp location.
2. Untar the file (tar.gz).  
3. As a root user, chmod +x the 3 shell script files.
3. Run the 'setup.sh'.  

This will create and install the `hadoopcli` application to your path.

Try it out on a host with default configs (if kerberized, get a ticket first):

    hadoopcli

To use an alternate HADOOP_CONF_DIR:

    hadoopcli --config /var/hadoop/dev-cfg

[Release Notes](./release_notes.md)

### Core Functions

### CLI Help
```
usage: hadoopcli
 -?,--help
 -d,--debug                 Debug Commands
 -i,--init <init set>       Initialize with set
 -p,--password <password>   Password
 -r,--run file <run>        Run File and Exit
 -s,--silent                Suppress Banner
 -stdin,--stdin             Run Stdin pipe and Exit
 -u,--username <username>   Username to log into gateway
 -v,--verbose               Verbose Commands
```

### File System Command Basics

The HadoopCli maintains a context to the local filesystem AND the target HDFS filesystem, once connected.  A 'path' context for HDFS is also managed and is treated as the 'current' working HDFS directory.

CLI commands against will consider the 'working' directory, unless the path element to the command starts with a '/'.

For example, notice how commands can be issued *without* a path element (unlike standard `hdfs dfs` commands).  The current HDFS working directory is assumed.

Path *Completion* is also available (via tab, just like `bash`) and consider the path working directory as a reference.

```
Connected: hdfs://HOME90
REMOTE: hdfs://HOME90/user/dstreev		LOCAL: file:/home/dstreev
hdfs-cli:$ ls
Found 17 items
drwx------   - dstreev hadoop          0 2019-05-15 02:00 /user/dstreev/.Trash
drwxr-xr-x   - dstreev hadoop          0 2019-05-06 09:34 /user/dstreev/.hiveJars
drwxr-xr-x   - dstreev hadoop          0 2019-04-16 15:06 /user/dstreev/.sparkStaging
drwx------   - dstreev hadoop          0 2019-05-14 10:56 /user/dstreev/.staging
-rw-r--r--   3 dstreev hadoop        903 2019-03-07 13:50 /user/dstreev/000000_0
drwxr-xr-x   - dstreev hadoop          0 2019-04-12 11:33 /user/dstreev/data
drwxr-xr-x   - dstreev hadoop          0 2018-08-10 12:19 /user/dstreev/datasets
-rw-r-----   3 dstreev hadoop          0 2019-05-15 11:48 /user/dstreev/hello.chuck
-rw-r-----   3 dstreev hadoop          0 2019-05-15 11:49 /user/dstreev/hello.ted
drwxr-x---   - dstreev hadoop          0 2019-05-04 16:20 /user/dstreev/hms_dump
-rw-r--r--   3 dstreev hadoop        777 2018-12-28 10:26 /user/dstreev/kafka-phoenix-cc-trans.properties
drwxr-xr-x   - dstreev hadoop          0 2019-04-03 16:37 /user/dstreev/mybase
drwxr-xr-x   - dstreev hadoop          0 2019-04-03 16:47 /user/dstreev/myexttable
drwxr-xr-x   - dstreev hadoop          0 2019-05-14 14:16 /user/dstreev/temp2
drwxr-xr-x   - dstreev hadoop          0 2019-05-14 16:52 /user/dstreev/test
drwxr-xr-x   - dstreev hadoop          0 2019-04-03 21:50 /user/dstreev/test_ext
drwxr-x---   - dstreev hadoop          0 2019-05-08 08:30 /user/dstreev/testme
REMOTE: hdfs://HOME90/user/dstreev		LOCAL: file:/home/dstreev
hdfs-cli:$ cd datasets
REMOTE: hdfs://HOME90/user/dstreev/datasets		LOCAL: file:/home/dstreev
hdfs-cli:$ ls
Found 2 items
drwxr-xr-x   - dstreev hadoop          0 2019-01-31 14:17 /user/dstreev/datasets/external
drwxr-xr-x   - hive    hadoop          0 2019-03-18 06:09 /user/dstreev/datasets/internal.db
REMOTE: hdfs://HOME90/user/dstreev/datasets		LOCAL: file:/home/dstreev
hdfs-cli:$
```

### Scripting Support

Being able to maintain an HDFS context/session across multiple commands saves a huge amount of time because we don't need to suffer the overhead of starting the jvm and getting an HDFS session established.

If you have 'more' than a few commands to run against HDFS, packaging those commands up and processing them at the same time can be a big deal.

There are 3 ways to do this.

##### 'init' startup option

Create a text file with the commands you want to run.  One command per line.  And include that at startup.

Create init.txt
```
ls
count -h /user/dstreev
du -h /hdp
```

Then initialize a 'hadoopcli' session with it:
`hadoopcli -i init.txt`

##### 'run' option

Exactly the same as the 'init' option that will 'exit' after completion.

##### 'stdin' option

Make 'hadoopcli' part of your bash pipeline.  Hadoopcli will process 'stdin' the same way it processes the 'run' option.


### Command Reference

#### Common Commands
| Command | Description |
|---|:-----| 
| help \[command\] |	display help information |
| put |	upload local files to the remote HDFS |
| get |	retrieve remote files from HDFS to Local Filesystem |

#### Remote (HDFS) Commands

| Command | Description |
|---|:-----| 
| cd | change current working directory |
| copyFromLocal |   |
| copyToLocal |   |
| ls |  list directory contents |
| rm |  delete files/directories |
| pwd |  print working directory path |
| cat |  print file contents |
| chown | change ownership |
| chmod | change permissions |
| chgrp | change group |
| head | print first few lines of a file |
| mkdir | create directories |
| count | Count the number of directories, files and bytes under the paths that match the specified file pattern. |
| stat |  Print statistics about the file/directory at <path> in the specified format. |
| tail |  Displays last kilobyte of the file to stdout. |
| test |  Validate Path |
| text | Takes a source file and outputs the file in text format. |
| touch/touchz | Create a file of zero length. |
| usage | Return the help for an individual command. |
| createSnapshot | Create Snapshot |
| deleteSnapshot | Delete Snapshot |
| renameSnapshot | Rename Snapshot |

#### Local (Local File System) Commands
| Command | Description |
|---|:-----|
| lcd | change current working directory |
| lls | list directory contents |
| lrm |  delete files/directories |
| lpwd | print working directory path |
| lcat | print file contents |
| lhead | print first few lines of a file |
| lmkdir | create directories |

#### Tools and Utilities
| Command | Description |
|---|:-----|
| lsp | ls plus.  Includes Block information and locations. |     
| nnstat | Namenode Statistics |


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
usage: lsp [OPTION ...] [ARGS ...]
Options:
 -c,--comment <comment>           Add comment to output
 -d,--maxDepth <maxDepth>         Depth of Recursion (default 5), use '-1'
                                  for unlimited
 -f,--format <output-format>      Comma separated list of one or more:
                                  permissions_long,replication,user,group,
                                  size,block_size,ratio,mod,access,path,da
                                  tanode_info (default all of the above)
 -F,--filter <filter>             Regex Filter of Content
 -i,--invisible                   Process Invisible Files/Directories
 -n,--newline <newline>           New Line
 -o,--output <output directory>   Output Directory (HDFS) (default
                                  System.out)
 -R,--recursive                   Process Path Recursively
 -s,--separator <separator>       Field Separator
 -v,--verbose                     show verbose output
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
	




