// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.Environment;

public class LocalPwd extends HdfsCommand {

    public LocalPwd(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        FileSystem localfs = (FileSystem)env.getValue(LOCAL_FS);
        Path path = localfs.getWorkingDirectory();
        System.out.println(path.toString().substring(5));
    }
}
