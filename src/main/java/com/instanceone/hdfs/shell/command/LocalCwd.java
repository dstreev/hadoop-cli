// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import java.io.IOException;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.Environment;

public class LocalCwd extends HdfsCommand {

    public LocalCwd(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try{
        String dir = cmd.getArgs().length == 0 ? System.getProperty("user.home") : cmd.getArgs()[0];
        System.out.println("Change Dir to: " + dir);
        System.out.println("CWD: " + localfs.getWorkingDirectory());
        Path newPath = null;
        if(dir.startsWith("~/")){
            dir = System.getProperty("user.home") + dir.substring(1);
        }
        System.out.println("Dir: " + dir);
        newPath = new Path(dir);
        
        Path qPath = localfs.makeQualified(newPath);
        System.out.println("Qual Path: " + qPath);
        
        if(localfs.getFileStatus(qPath).isDir() && localfs.exists(qPath)){
            localfs.setWorkingDirectory(qPath);
        } else{
            System.out.println("No such directory: " + dir);
        }
        } catch(IOException e){
            System.out.println(e.getMessage());
        }

    }

}
