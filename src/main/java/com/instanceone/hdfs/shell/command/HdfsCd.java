// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import java.io.IOException;
import java.net.URI;

import jline.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.AbstractCommand;
import com.instanceone.hdfs.shell.Environment;

public class HdfsCd extends HdfsCommand {

    public HdfsCd(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            FileSystem hdfs = super.getFileSystem(env, reader);
            String hdfsUrl = super.hdfsUrl(env, reader);
            String cwd = super.cwd(env, reader);
            String dir = cmd.getArgs().length == 0 ? "/" : cmd.getArgs()[0];
            log(cmd, "CWD before: " + cwd);
            log(cmd, "Requested CWD: " + dir);
            
            Path path = new Path(cwd, dir);
            
            Path qPath = path.makeQualified(hdfs);
            if(hdfs.getFileStatus(qPath).isDir() && hdfs.exists(qPath)){
                String qpStr = qPath.toString();
                String newCwd = qpStr.substring(hdfsUrl.length() -1);
                log(cmd, "New CWD: " + newCwd);
                env.setProperty(HDFS_CWD, newCwd);
            } else{
                System.out.println("No such directory: " + dir);
            }
            
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
    
}
