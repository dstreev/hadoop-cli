// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import java.io.File;

import jline.ConsoleReader;

import org.apache.commons.cli.CommandLine;

import com.instanceone.hdfs.shell.AbstractCommand;
import com.instanceone.hdfs.shell.Environment;

public class LocalLs extends AbstractCommand {

    public LocalLs(String name) {
        super(name);
    }

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
    
    public static void main(String[] args){
        String name = "foobar.xml";
        System.out.println(name.matches("foobar.xml"));
    }

}
