// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public abstract class AbstractCommand implements Command {
    private String name;
    
    public AbstractCommand(String name){
        this.name = name;
    }

    public String getHelpHeader() {
        return "Options:";
    }

    public String gethelpFooter() {
        return null;
    }

    public String getName() {
        return name;
    }


    public Options getOptions() {
        Options opts =  new Options();
        opts.addOption("v", "verbose", false, "show verbose output");
        return opts;
    }
    
    public String getUsage(){
        return getName() + " [OPTION ...] [ARGS ...]";
    }
    
    protected void log(CommandLine cmd, String log){
        if(cmd.hasOption("v")){
            System.out.println(log);
        }
    }

}
