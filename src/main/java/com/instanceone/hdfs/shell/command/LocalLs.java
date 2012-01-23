// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import static com.instanceone.hdfs.shell.command.FSUtil.*;

import java.io.IOException;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.Environment;

public class LocalLs extends HdfsCommand {

    public LocalLs(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
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
    
    /*
    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        String cwd = env.getProperty(Environment.CWD);
        if(cwd == null){
            cwd = System.getProperty("user.dir");
            env.setProperty(Environment.CWD, cwd);
        }
        if(cmd.getArgs().length > 0){
            cwd += "/" + cmd.getArgs()[0];
        }
        
        File cwdFile = new File(cwd);
        System.out.println(cwdFile.getAbsolutePath());
        File[] files = cwdFile.listFiles();
        System.out.println(files);
        for(File file : files){
            System.out.println(file.getName());
        }

    }
    */
    
    
}
