## HCFS Browser

HCFS Browser is an interactive command line shell that makes interacting with the Hadoop Compatible File System (HCFS) simpler and more intuitive than the standard command-line tools that come with Hadoop. If you're familiar with OS X, Linux, or even Windows terminal/console-based applications, then you are likely familiar with features such as tab completion, command history, and ANSI formatting.

### Default or Alt JDK Usage

The startup script `hcfs-browser` will use `$JAVA_HOME` if defined. When it is not available, the default `java` implementation will be used.

JDK 17 or above is recommended. JDK's below 1.8.0_100 are not recommended for Kerberos environments and may have issues connecting to secure clusters.

If Kerberos connections aren't working, use a more recent JDK by setting the `$JAVA_HOME` variable.

### AUX_LIBS - CLASSPATH Additions

The directory `$HOME/.hcfs-browser/aux_libs` will be scanned for 'jar' files. Each 'jar' will be added to the Java classpath of the application. Add any required libraries here.

The application contains all the necessary `hdfs` classes already. You will need to add to the `aux_libs` directory the following:
- AWS S3 Drivers (appropriate versions)
    - `hadoop-aws.jar`
    - `aws-java-sdk-bundle.jar`

### Core Functions

### CLI Help

```
usage: hcfs-browser
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

HCFS Browser maintains a context to the local filesystem AND the target HDFS filesystem, once connected. A 'path' context for HDFS is also managed and is treated as the 'current' working HDFS directory.

CLI commands will consider the 'working' directory, unless the path element to the command starts with a '/'.

For example, notice how commands can be issued *without* a path element (unlike standard `hdfs dfs` commands). The current HDFS working directory is assumed.

Path *Completion* is also available (via tab, just like `bash`) and considers the working directory as a reference.

```
Connected: hdfs://HOME90
REMOTE: hdfs://HOME90/user/dstreev		LOCAL: file:/home/dstreev
hcfs-browser:$ ls
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
hcfs-browser:$ cd datasets
REMOTE: hdfs://HOME90/user/dstreev/datasets		LOCAL: file:/home/dstreev
hcfs-browser:$ ls
Found 2 items
drwxr-xr-x   - dstreev hadoop          0 2019-01-31 14:17 /user/dstreev/datasets/external
drwxr-xr-x   - hive    hadoop          0 2019-03-18 06:09 /user/dstreev/datasets/internal.db
REMOTE: hdfs://HOME90/user/dstreev/datasets		LOCAL: file:/home/dstreev
hcfs-browser:$
```

### Variable Support

Commandline input is processed for variable matching. IE: ${VARNAME} or $VARNAME. Use the `env` command for a list of variables available. Additional variables can be added two ways:

- By a `java` properties file, which is referenced for input by `-ef`
- Using the `env -s` (set) command to add them dynamically within the session.

Default behavior of the startup script will look for `${HOME}/.hcfs-browser/env-var.props` and load this automatically, if it exists. If you have a common set of variables you wish to persist between sessions, add them to this file.

Example: `env-var.props`
```
HEW=/warehouse/tablespace/external/hive
HMW=/warehouse/tablespace/managed/hive
```

### Scripting Support

Being able to maintain an HDFS context/session across multiple commands saves a huge amount of time because we don't need to suffer the overhead of starting the JVM and getting an HDFS session established.

If you have 'more' than a few commands to run against HDFS, packaging those commands up and processing them at the same time can be a big deal.

There are 3 ways to do this.

##### 'init' startup option `-i <file>`

Create a text file with the commands you want to run. One command per line. And include that at startup.

Create init.txt

```
ls
count -h /user/dstreev
du -h /hdp
```

Then initialize a session with it:

```
hcfs-browser -i init.txt
```

##### 'execute' option `-e <command>`

Exactly the same as the 'init' option that will 'exit' after completion.

##### 'stdin' option `-stdin`

Make HCFS Browser part of your bash pipeline. It will process 'stdin' the same way it processes the 'run' option.

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
| stat |  Print statistics about the file/directory at \<path\> in the specified format. |
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

* [lsp](./lsp.md) - *ls plus*. Includes Block information and locations.
* [nnstats](./nnstats.md) - Namenode Stats

### Basic Usage

HCFS Browser works much like a command-line ftp client: You first establish a connection to a remote HDFS filesystem, then manage local/remote files and transfers.

To use an alternate HADOOP_CONF_DIR:

```
hcfs-browser --config /var/hadoop/dev-cfg
```

### Command Documentation

Help for any command can be obtained by executing the `help` command:

```
help pwd
```

#### Local vs. Remote Commands

When working within an HCFS Browser session, you manage both local (on your computer) and remote (HDFS) files. By convention, commands that apply to both local and remote filesystems are differentiated by prepending an `l` character to the name to denote "local".

For example:

`lls` lists local files in the local current working directory.

`ls` lists remote files in the remote current working directory.

Every HCFS Browser session keeps track of both the local and remote current working directories.

### Support for External Configurations (core-site.xml, hdfs-site.xml)

By default, HCFS Browser will use the `--config` option to specify the location of `core-site.xml` and `hdfs-site.xml`.

The `--config` option takes 1 parameter, a local directory. This directory should contain hdfs-site.xml and core-site.xml files. When used, you'll automatically be connected to HDFS and changed to your HDFS home directory.

Example Connection parameters.

``` bash
# Use the hadoop files in the input directory to configure and connect to HDFS.
hcfs-browser --config ../mydir
```

This can be used in conjunction with the 'Startup' Init option below to run a set of commands automatically after the connection is made. The 'connect' option should NOT be used in the initialization script.

### Startup Initialization Option

Using the option `-i <filename>` when launching the CLI, it will run all the commands in the file.

The file needs to be located in the `$HOME/.hcfs-browser` directory. For example:

``` bash
hcfs-browser -i test
```

Will initialize the session with the command(s) in `$HOME/.hcfs-browser/test`. One command per line.

The contents could be any set of valid commands that you would use in the CLI. For example:

```
cd user/dstreev
```

### Building

```
mvn -DskipTests clean install package
```

### Scheduler Stats

Collect Queue Stats from the YARN REST API.

```
usage: sstat
 -d,--delimiter <arg>     Record Delimiter (Cntrl-A is default).
 -e,--end <arg>           End time for retrieval in 'yyyy-MM-dd HH:mm:ss'
 -ff,--fileFormat <arg>   Output filename format.  Value must be a pattern
                          of 'SimpleDateFormat' format options.
 -h,--help                Help
 -hdr,--header            Print Record Header
 -inc,--increment <arg>   Query Increment in minutes
 -l,--last <arg>          last x-DAY(S)|x-HOUR(S)|x-MIN(S). 1-HOUR=1 hour,
                          2-DAYS=2 days, 3-HOURS=3 hours, etc.
 -o,--output <arg>        Output Base Directory (HDFS) (default
                          System.out) from which all other sub-directories
                          are based.
 -raw,--raw               Raw Record Output
 -s,--start <arg>         Start time for retrieval in 'yyyy-MM-dd
                          HH:mm:ss'
 -ssl,--ssl               https connection
 -v,--verbose             show verbose output
```

### Container Stats

Collect Container Stats from the YARN REST API.

```
usage: cstat
 -d,--delimiter <arg>     Record Delimiter (Cntrl-A is default).
 -e,--end <arg>           End time for retrieval in 'yyyy-MM-dd HH:mm:ss'
 -ff,--fileFormat <arg>   Output filename format.  Value must be a pattern
                          of 'SimpleDateFormat' format options.
 -h,--help                Help
 -hdr,--header            Print Record Header
 -inc,--increment <arg>   Query Increment in minutes
 -l,--last <arg>          last x-DAY(S)|x-HOUR(S)|x-MIN(S). 1-HOUR=1 hour,
                          2-DAYS=2 days, 3-HOURS=3 hours, etc.
 -o,--output <arg>        Output Base Directory (HDFS) (default
                          System.out) from which all other sub-directories
                          are based.
 -raw,--raw               Raw Record Output
 -s,--start <arg>         Start time for retrieval in 'yyyy-MM-dd
                          HH:mm:ss'
 -ssl,--ssl               https connection
 -v,--verbose             show verbose output
```
