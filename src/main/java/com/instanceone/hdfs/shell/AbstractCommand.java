// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell;



import java.util.ArrayList;
import java.util.List;

import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public abstract class AbstractCommand implements Command{
    private String name;
    private ArrayList<Completer> completers = new ArrayList<Completer>();
    
    
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

    public List<Completer> getCompleters() {
        return this.completers;
    }


    
    

}
