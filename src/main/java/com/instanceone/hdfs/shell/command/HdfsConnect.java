// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import java.io.IOException;
import java.util.List;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileSystem;

import com.instanceone.hdfs.shell.Environment;

public class HdfsConnect extends HdfsCommand {

    public HdfsConnect(String name) {
        super(name);
        Completer completer = new StringsCompleter("hdfs://localhost:9000/", "hdfs://dlcirrus01:9000/");
        this.completer = completer;
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
