// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;

import com.instanceone.hdfs.shell.Environment;

public class LocalPwd extends HdfsCommand {

    public LocalPwd(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        FileSystem localfs = (FileSystem)env.getValue(LOCAL_FS);
        
        String wd = localfs.getWorkingDirectory().toString();
        if (cmd.hasOption("l")) {
            System.out.println(wd);
        }
        else {
            // strip off prefix: "file:"
            System.out.println(wd.substring(5));
        }
    }
    
    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("l", false, "show the full file system URL");
        return opts;
    }
}
