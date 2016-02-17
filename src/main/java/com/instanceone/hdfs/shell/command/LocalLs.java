// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import static com.instanceone.hdfs.shell.command.FSUtil.longFormat;
import static com.instanceone.hdfs.shell.command.FSUtil.shortFormat;

import java.io.IOException;

import com.dstreev.hdfs.shell.command.Constants;
import jline.console.ConsoleReader;
import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.completers.FileSystemNameCompleter;
import com.instanceone.stemshell.Environment;

public class LocalLs extends HdfsCommand {
    private Environment env;

    public LocalLs(String name, Environment env) {
        super(name, env);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            FileSystem localfs = (FileSystem)env.getValue(Constants.LOCAL_FS);
            Path srcPath = cmd.getArgs().length == 0 ? localfs.getWorkingDirectory() : new Path(localfs.getWorkingDirectory(), cmd.getArgs()[0]);
            FileStatus[] files = localfs.listStatus(srcPath);
            for (FileStatus file : files) {
                if (cmd.hasOption("l")) {
                    log(cmd, longFormat(file));
                }
                else {
                    log(cmd, shortFormat(file));
                }
            }
            FSUtil.prompt(env);
        }
        catch (IOException e) {
            log(cmd, e.getMessage());
        }
    }


    @Override
    public Options getOptions() {
        // TODO Auto-generated method stub
        Options opts = super.getOptions();
        opts.addOption("l", false, "show extended file attributes");
        return opts;
    }  
    
    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, true);
    }
    
    
}
