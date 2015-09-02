// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import com.instanceone.stemshell.Environment;
import com.instanceone.stemshell.command.AbstractCommand;
import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;

public class HdfsUGI extends HdfsCommand {

    public HdfsUGI(String name) {
        super(name);
//        Completer completer = new StringsCompleter("hdfs://localhost:8020/", "hdfs://hdfshost:8020/");
//        this.completer = completer;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            UserGroupInformation.main(cmd.getArgs());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Completer getCompleter() {
        return this.completer;
    }


}
