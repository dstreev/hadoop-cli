// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import java.io.IOException;
import java.net.URI;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.Environment;

public class HdfsConnect extends HdfsCommand {

    public HdfsConnect(String name) {
        super(name);
        Completer completer = new StringsCompleter("hdfs://localhost:9000/", "hdfs://dlcirrus01:9000/");
        this.completer = completer;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            
           
            
//            env.setProperty(HDFS_URL, null);
//            env.setProperty(HDFS_CWD, null);
            
            if(cmd.getArgs().length > 0){
                Configuration config = new Configuration();
                FileSystem hdfs = FileSystem.get(URI.create(cmd.getArgs()[0]),
                                config);
//                HdfsCommand.hdfs = hdfs;
                env.setValue(HDFS, hdfs);
                // set working dir to root
                hdfs.setWorkingDirectory(hdfs.makeQualified(new Path("/")));
                
                FileSystem local = FileSystem.getLocal(new Configuration());
//                HdfsCommand.localfs = local;
                env.setValue(LOCAL_FS, local);
                env.setProperty(HDFS_URL, hdfs.getUri().toString());
                
                
                System.out.println("Connected HDFS Uri: " + hdfs.getUri());
                System.out.println("HDFS CWD: " + hdfs.getWorkingDirectory());
                System.out.println("Local FS Uri: " + local.getUri());
                System.out.println("Local CWD: " + local.getWorkingDirectory());
                
                //env.setProperty(HDFS_URL, cmd.getArgs()[0]);
                //System.out.println("HDFS Connected: " + cmd.getArgs()[0]);
            }
            
//            FileSystem hdfs = super.getFileSystem(env, reader);
//            String hdfsUrl = super.hdfsUrl(env, reader);
            
            
            
            
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    
}
