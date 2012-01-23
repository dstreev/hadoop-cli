// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import java.io.IOException;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.Environment;

public class HdfsCd extends HdfsCommand {

    public HdfsCd(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            String dir = cmd.getArgs().length == 0 ? "/" : cmd.getArgs()[0];
            log(cmd, "CWD before: " + hdfs.getWorkingDirectory());
            log(cmd, "Requested CWD: " + dir);   
            
            Path newPath = null;
            if(dir.startsWith("/")){
                newPath = new Path(env.getProperty(HDFS_URL), dir);
            } else{
                newPath = new Path(hdfs.getWorkingDirectory(), dir);
            }

            Path qPath = newPath.makeQualified(hdfs);
            System.out.println(newPath);
            if (hdfs.getFileStatus(qPath).isDir() && hdfs.exists(qPath)) {
                hdfs.setWorkingDirectory(qPath);
            }
            else {
                System.out.println("No such directory: " + dir);
            }
            
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
    
}
