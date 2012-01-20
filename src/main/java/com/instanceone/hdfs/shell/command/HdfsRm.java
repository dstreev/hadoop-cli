// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import java.io.File;
import java.io.IOException;

import jline.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.AbstractCommand;
import com.instanceone.hdfs.shell.Environment;

public class HdfsRm extends HdfsCommand {

    public HdfsRm(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {

        try {
            String cwd = super.cwd(env, reader);
            //String lcwd = env.getProperty(Environment.CWD);
            String hdfsCwd = env.getProperty(HDFS_CWD);
            FileSystem fs = super.getFileSystem(env, reader);
            
            log(cmd, "HDFS Dir: " + hdfsCwd);
            
            String file = cmd.getArgs()[0];

            String remoteFile = cmd.getArgs()[0];

            log(cmd, "HDFS file: " + remoteFile);
            Path hdfsPath = new Path(hdfsCwd, remoteFile);
            log(cmd, "Remote path: " + hdfsPath);

            boolean recursive = cmd.hasOption("r");
            fs.delete(hdfsPath, recursive);

        }
        catch (IOException e) {
            System.out.println("Error: " + e.getMessage());
        }

    }

    @Override
    public Options getOptions() {
        Options opts =  super.getOptions();
        opts.addOption("r", false, "delete recursively");
        return opts;
    }
    
    

}
