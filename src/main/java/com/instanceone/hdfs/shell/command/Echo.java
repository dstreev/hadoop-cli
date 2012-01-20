// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import jline.ConsoleReader;

import org.apache.commons.cli.CommandLine;

import com.instanceone.hdfs.shell.AbstractCommand;
import com.instanceone.hdfs.shell.Environment;


public class Echo extends AbstractCommand {

    public Echo(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
       String[] args = cmd.getArgs();
       StringBuffer sb = new StringBuffer();
       for(int i = 0;i< args.length; i++){
           sb.append(args[i]);
           if((i+1) != args.length){
               sb.append(" ");
           }
       }
       System.out.println(sb.toString());
        
    }

}
