## HADOOP-CLI

HADOOP-CLI is an interactive command line shell that makes interacting with the Hadoop Distribted Filesystem (HDFS)
simpler and more intuitive than the standard command-line tools that come with Hadoop. If you're familiar with OS X, Linux, or even Windows terminal/console-based applications, then you are likely familiar with features such as tab completion, command history, and ANSI formatting.

### Binary Package

NOTE: The prebuilt binary was compiled against HDFS 3.1, but is backwardly compatible with HDFS 2.6.x and HDFS 2.7.x (HDP 2.5.x, HDP 2.6.x)

### Don't Build, Download the LATEST binary here!!!
[![Download the LATEST Binary](./images/download.png)](https://github.com/dstreev/hadoop-cli/releases)

* Download the release 'tar.gz' file to a temp location.
* Untar the file (tar.gz).
```
tar xzvf <release>.tar.gz
cd hadoop-cli
```  
* As a root user, chmod +x the 3 shell script files.
* Run the 'setup.sh'.
```
./setup
```  

This will create and install the `hadoopcli` application to your path.

Try it out on a host with default configs (if kerberized, get a ticket first):

    hadoopcli

To use an alternate HADOOP_CONF_DIR:

    hadoopcli --config /var/hadoop/dev-cfg

### AUX_LIBS - CLASSPATH Additions

The directory `$HOME/.hadoop-cli/aux_libs` will be scanned for 'jar' files. Each 'jar' will be added the java classpath of the application.  Add any required libaries here.

The application contains all the necesasry `hdfs` classes already.  You will need to add to the `aux_libs` directory the following:
- AWS S3 Drivers (appropriate versions)
    - `hadoop-aws.jar`
    - `aws-java-sdk-bundle.jar`

### [Release Notes](./release_notes.md)

### Core Functions

### CLI Help

```
usage: hadoopcli
 -api,--api                       API mode
 -d,--debug                       Debug Commands
 -e,--execute <arg>               Execute Command
 -ef,--env-file <arg>             Environment File(java properties format)
                                  with a list of key=values
 -f,--file <arg>                  File to execute
 -h,--help                        Help
 -i,--init <arg>                  Initialization with Set
 -s,--silent                      Suppress Banner
 -stdin,--stdin                   Run Stdin pipe and Exit
 -t,--template <arg>              Template to apply on input (-f | -stdin)
 -td,--template-delimiter <arg>   Delimiter to apply to 'input' for
                                  template option (default=',')
 -v,--verbose                     Verbose Commands
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

### Variable Support

Commandline input is processed for variables matching. IE: ${VARNAME} or $VARNAME.  Use the 'env' command for a list of variables available.  Additional variables can be added two ways:

- By a `java` properties file, which is referenced for input by `-ef`
- Using the `env -s` (set) command to add them dynamically within the session.

Default behavior of the startup script will look for `${HOME}/.hadoop-cli/env-var.props` and load this automatically, if it exists.  If you have a common set of variable you wish to persist between session, add them to this file.

Example: `env-var.props`
```
HEW=/warehouse/tablespace/external/hive
HMW=/warehouse/tablespace/managed/hive
```

### Scripting Support

Being able to maintain an HDFS context/session across multiple commands saves a huge amount of time because we don't need to suffer the overhead of starting the jvm and getting an HDFS session established.

If you have 'more' than a few commands to run against HDFS, packaging those commands up and processing them at the same time can be a big deal.

There are 3 ways to do this.

##### 'init' startup option `-i <file>`

Create a text file with the commands you want to run.  One command per line.  And include that at startup.

Create init.txt

```
ls
count -h /user/dstreev
du -h /hdp
```

Then initialize a 'hadoopcli' session with it:

```
hadoopcli -i init.txt
```

##### 'execute' option `-e <command>`

Exactly the same as the 'init' option that will 'exit' after completion.

##### 'stdin' option `-stdin`

Make 'hadoopcli' part of your bash pipeline.  Hadoopcli will process 'stdin' the same way it processes the 'run' option.

### Command Reference

#### Help Commands

| Command | Description |
|---|:-----|
| env | Env variable command |
| help | List all available commands |
| help \[command\] |	display help information |

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

* [lsp](./lsp.md) - *ls plus*.  Includes Block information and locations.
* [nnstats](./nnstats.md) - Namenode Stats

### Building

This project requires the artifacts from https://github.com/dstreev/stemshell , which is a forked enhancement that has added support for processing command line parameters and deals with quoted variables.

Since we're now doing more in the interface and writing results to hdfs, we need to build binary compatible packages.  The default `mvn` profile is for Apache Hadoop 3.1.  There is a profile for Apache Hadoop 2.6 and 2.7.

```
# For 3.1
mvn -DskipTests clean install package
```

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

To start HADOOP-CLI, run the following command (after running setup as described above):

```
hadoopcli
```
		
### Command Documentation

Help for any command can be obtained by executing the `help` command:

```
help pwd
```

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

``` bash
# Use the hadoop files in the input directory to configure and connect to HDFS.
hadoopcli --config ../mydir
```

This can be used in conjunction with the 'Startup' Init option below to run a set of commands automatically after the connection is made.  The 'connect' option should NOT be used in the initialization script.

### Startup Initialization Option

Using the option '-i <filename>' when launching the CLI, it will run all the commands in the file.

The file needs to be location in the $HOME/.hadoop-cli directory.  For example:

``` bash
# If you're using the helper shell script
hadoopcli -i test
	
# If you're using the java command
java -jar hadoop-cli-full-bin.jar -i test
```

Will initialize the session with the command(s) in $HOME/.hadoop-cli/test. One command per line.

The contents could be any set of valid commands that you would use in the cli. For example:

```
cd user/dstreev
```


### Job History Stats
	
Delivered - Docs to come

### Scheduler Stats

Delivered - Docs to come

### Container Stats

Delivered - Docs to come
	
### Road Map

See [Issues](./ISSUES)	




