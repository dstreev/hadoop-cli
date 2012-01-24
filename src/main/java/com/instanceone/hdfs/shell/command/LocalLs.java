// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import static com.instanceone.hdfs.shell.command.FSUtil.*;

import java.io.IOException;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.Environment;
import com.instanceone.hdfs.shell.completers.FileSystemNameCompleter;

public class LocalLs extends HdfsCommand {
    private Environment env;

    public LocalLs(String name, Environment env) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            FileSystem localfs = (FileSystem)env.getValue(LOCAL_FS);
            Path srcPath = cmd.getArgs().length == 0 ? localfs.getWorkingDirectory() : new Path(localfs.getWorkingDirectory(), cmd.getArgs()[0]);
            FileStatus[] files = localfs.listStatus(srcPath);
            for (FileStatus file : files) {
                // String fileName = file.getPath().
                if (cmd.hasOption("l")) {
                    System.out.println(longFormat(file));
                }
                else {
                    System.out.println(shortFormat(file));
                }
            }
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
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
