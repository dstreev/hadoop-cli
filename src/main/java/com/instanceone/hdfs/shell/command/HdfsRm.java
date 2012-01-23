// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.Environment;

public class HdfsRm extends HdfsCommand {

    public HdfsRm(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {

        try {
            String remoteFile = cmd.getArgs()[0];

            log(cmd, "HDFS file: " + remoteFile);
            Path hdfsPath = new Path(hdfs.getWorkingDirectory(), remoteFile);
            log(cmd, "Remote path: " + hdfsPath);

            boolean recursive = cmd.hasOption("r");
            hdfs.delete(hdfsPath, recursive);

        }
        catch (Throwable e) {
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
