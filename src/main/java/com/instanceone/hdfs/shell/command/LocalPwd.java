// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import com.dstreev.hdfs.shell.command.Constants;
import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;

import com.instanceone.stemshell.Environment;

public class LocalPwd extends HdfsCommand {

    public LocalPwd(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        FileSystem localfs = (FileSystem)env.getValue(Constants.LOCAL_FS);
        
        String wd = localfs.getWorkingDirectory().toString();
        if (cmd.hasOption("l")) {
            log(cmd, wd);
        }
        else {
            // strip off prefix: "file:"
            log(cmd, wd.substring(5));
        }
        FSUtil.prompt(env);
    }
    
    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("l", false, "show the full file system URL");
        return opts;
    }
}
