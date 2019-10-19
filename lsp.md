# Function 'lsp'

Like 'ls', you can fetch many details about a file.  But with this, you can also add information about the file that includes:
- Block Size
- Access Time
- Ratio of File Size to Block
- Datanode information for the files blocks (Host and Block Id)

`lsp` can be used to search hdfs, similar to the `find` linux program.  Although the syntax is a bit different.  The options `-F`,`-Fe`, and `-i`. 

`lsp` can be used to output a formatted row for files and directories using the `-f` option.  When the `datanode_info` option is specified, the output will contain details for each replicated block of a file.

## Options

```
usage: lsp [OPTION ...] [ARGS ...]
Options:
 -c,--comment <comment>                  Add comment to output
 -d,--maxDepth <maxDepth>                Depth of Recursion (default 5),
                                         use '-1' for unlimited
 -do,--dir-only                          Show Directories Only
 -f,--format <output-format>             Comma separated list of one or
                                         more:
                                         permissions_long,replication,user
                                         ,group,size,block_size,ratio,mod,
                                         access,path,file,datanode_info
                                         (default all of the above)
 -F,--filter <filter>                    Regex Filter of Content. Can be
                                         'Quoted'
 -Fe,--filter-element <filter element>   Filter on 'element'.  One of
                                         '--format'
 -i,--invisible                          Process Invisible
                                         Files/Directories
 -n,--newline <newline>                  New Line
 -o,--output <output directory>          Output Directory (HDFS) (default
                                         System.out)
 -R,--recursive                          Process Path Recursively
 -r,--relative                           Show Relative Path Output
 -s,--separator <separator>              Field Separator
 -sp,--show-parent                       For Test, show parent
 -t,--test                               Test for existence
 -v,--invert                             Invert Regex Filter of Content
```                                                                    

## Actions

### Recursion

Use the `-R` to recurse through directories.  Use the `-d` option to specify the depth of the recursion.  The default is 5.  Use -1 for no-limit, but be careful because this could iterate through the whole filesystem.  And that's not productive on 'large' filesystem.

```
lsp -R
```

Control the depth of the recursion.

```
lsp -R -d 2
```

### Filtering and Output

Find files matching a certain pattern, recursively from path context. The `-F` option takes a 'regex' expression.  By default, the 'path' `-f` option is searched.  If you'd like to search another element use `-Fe` and specify one of the valid `-f` options.

```
lsp -R -F .*trans.*
```

The output will include the standard output `-f`, which maybe quite verbose.

Limit the result output like:

```
lsp -R -F .*trans.* -f path
```

Try using the `-do` option to list ONLY directories.

```
lsp -R -F .*trans.* -f path -do
```                            

### Using Comments

The `-c` option will prepend any output with the comment.  It's a great way to drive scripts from the results.

```
lsp -R -c 'count -h' -F .*trans.* -f path -do
```       

### How about an Inverted Search

There are times you want to find files that do *NOT* match a pattern. Use the `-v` option to reverse match on the filter.

```
lsp -F *.trans.* -v
```        

### Default Output

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

