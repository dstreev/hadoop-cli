// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;

import com.instanceone.hdfs.shell.Environment;

public class HdfsPwd extends HdfsCommand {

    public HdfsPwd(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        FileSystem hdfs = (FileSystem) env.getValue(HDFS);
        String wd = hdfs.getWorkingDirectory().toString();
        if (cmd.hasOption("l")) {
            log(cmd, wd);
        }
        else {
            log(cmd, wd.substring(env.getProperty(HDFS_URL).length()));
        }

    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("l", false, "show the full HDFS URL");
        return opts;
    }

}
