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

public class HdfsConnect extends HdfsCommand {

    public HdfsConnect(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            
            env.setProperty(HDFS_URL, null);
            env.setProperty(HDFS_CWD, null);
            
            if(cmd.getArgs().length > 0){
                env.setProperty(HDFS_URL, cmd.getArgs()[0]);
            }
            
            FileSystem hdfs = super.getFileSystem(env, reader);
            String hdfsUrl = super.hdfsUrl(env, reader);
            
            System.out.println("HDFS Connected: " + hdfsUrl);
            
            
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
    
}
