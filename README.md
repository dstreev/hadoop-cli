## HDFS-CLI

HDFS-CLI is an interactive command line shell that makes interacting with the Hadoop Distribted Filesystem (HDFS)
simpler and more intuitive than the standard command-line tools that come with Hadoop. If you are familiar with OS X, Linux, or even Windows terminal/console-based applications, then you are likely familiar with features such as tab completion, command history, and ANSI formatting.

### Basic Usage
HDFS-CLI works much like a command-line ftp client: You first establish a connection to a remote HDFS filesystem,
then manage local/remote files and transfers.

To start HDFS-CLI, run the following command:

	java -jar hdfs-cli-0.0.1-SNAPSHOT.jar

#### Local vs. Remote Commands
When working within a HDFS-CLI session, you manage both local (on your computer) and remote (HDFS) files. By convention, commands that apply to both local and remote filesystems are differentiated by prepending an `l`
character to the name to denote "local".

For example:

`lls` lists local files in the local current working directory.

`ls` lists remote files in the remote current working directory.

Every HDFS-CLI session keeps track of both the local and remote current working directories.


### Available Commands

#### Common Commands
	connect		connect to a remote HDFS instance
	help		display help information
	put			upload local files to the remote HDFS


####Remote (HDFS) Commands
	cd		 change current working directory
	ls		 list directory contents
	rm		 delete files/directories
	pwd		 print working directory path
	cat		 print file contents
	head	 print first few lines of a file
	mkdir	 create directories


#### Local (Local File System) Commands
	lcd		 change current working directory
	lls		 list directory contents
	lrm		 delete files/directories
	lpwd	 print working directory path
	lcat	 print file contents
	lhead	 print first few lines of a file
	lmkdir	 create directories
	
### Command Documentation
Help for any command can be obtained by executing the `help` command:

	help pwd

Note that currently, documentation may be limited.


### Known Bugs/Limitations

* No support for paths containing spaces
* No support for Windows XP




