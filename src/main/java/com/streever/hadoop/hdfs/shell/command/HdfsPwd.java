// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.streever.hadoop.hdfs.shell.command;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;

import com.streever.tools.stemshell.Environment;

public class HdfsPwd extends HdfsCommand {

    public HdfsPwd(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        FileSystem hdfs = (FileSystem) env.getValue(Constants.HDFS);
        String wd = hdfs.getWorkingDirectory().toString();
        if (cmd.hasOption("l")) {
            log(cmd, wd);
        }
        else {
            log(cmd, wd.substring(env.getProperty(Constants.HDFS_URL).length()));
        }
        FSUtil.prompt(env);

    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("l", false, "show the full HDFS URL");
        return opts;
    }

}
