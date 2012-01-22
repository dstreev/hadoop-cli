// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import java.io.IOException;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.instanceone.hdfs.shell.Environment;

public class HdfsPwd extends HdfsCommand {

    public HdfsPwd(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        String cwd;
        try {
            cwd = super.cwd(env, reader);
            if(cmd.hasOption("l")){
                System.out.println(super.hdfsUrl(env, reader) + cwd.substring(1));
            } else{
                System.out.println(cwd);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Options getOptions() {
        Options opts =  super.getOptions();
        opts.addOption("l", false, "show the full HDFS URL");
        return opts;
    }
    
    

}
