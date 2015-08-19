// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.stemshell.Environment;

public class LocalRm extends HdfsCommand {
    private boolean local = false;

    public LocalRm(String name, boolean local) {
        super(name);
        this.local = local;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            FileSystem hdfs = this.local ? (FileSystem) env.getValue(LOCAL_FS)
                            : (FileSystem) env.getValue(HDFS);
            String remoteFile = cmd.getArgs()[0];

            logv(cmd, "HDFS file: " + remoteFile);
            Path hdfsPath = new Path(hdfs.getWorkingDirectory(), remoteFile);
            logv(cmd, "Remote path: " + hdfsPath);

            boolean recursive = cmd.hasOption("r");
            logv(cmd, "Deleting recursively...");
            hdfs.delete(hdfsPath, recursive);

            FSUtil.prompt(env);

        }
        catch (Throwable e) {
            log(cmd, "Error: " + e.getMessage());
        }

    }

    @Override
    public Options getOptions() {
        Options opts =  super.getOptions();
        opts.addOption("r", false, "delete recursively");
        return opts;
    }
}
