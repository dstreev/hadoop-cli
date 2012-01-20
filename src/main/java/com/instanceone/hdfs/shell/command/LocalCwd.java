// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import java.io.File;

import jline.ConsoleReader;

import org.apache.commons.cli.CommandLine;

import com.instanceone.hdfs.shell.AbstractCommand;
import com.instanceone.hdfs.shell.Environment;

public class LocalCwd extends AbstractCommand {

    public LocalCwd(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        
        String dir = cmd.getArgs().length == 0 ? "~/" : cmd.getArgs()[0];
        String cwd = env.getProperty(Environment.CWD);
        if (cwd == null) {
            cwd = System.getProperty("user.dir");
            env.setProperty(Environment.CWD, cwd);
        }
        File cwdFile = new File(cwd);
        log(cmd,"CWD before: " + cwdFile.getAbsolutePath());
        
        log(cmd,"Requested CWD to: " + dir);
        File newDir = null;
        if(dir.startsWith("/")){
            newDir = new File(dir);
            
        } else if (dir.startsWith("~/")){
            String subDir = dir.substring(2);
            newDir = new File(System.getProperty("user.home"), subDir);
        } else{
            newDir = new File(cwdFile, dir);
        }
//        File 
        log(cmd,"new CWD: " + newDir.getAbsolutePath());
        if (!newDir.exists() || newDir.isFile()) {
            System.out.println("Not a directory: " + dir);
        }
        else if(newDir.isDirectory()) {
            try {
                log(cmd,"Changing CWD to: "
                                + newDir.getCanonicalPath());
                env.setProperty(Environment.CWD, newDir.getCanonicalPath());
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
