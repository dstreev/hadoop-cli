// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import java.io.IOException;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.Environment;
import com.instanceone.hdfs.shell.completers.FileSystemNameCompleter;

public class HdfsCd extends HdfsCommand {
    private Environment env;

    public HdfsCd(String name, Environment env) {
        super(name);
        this.env = env;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            FileSystem hdfs = (FileSystem)env.getValue(HDFS);
            
            String dir = cmd.getArgs().length == 0 ? "/" : cmd.getArgs()[0];
            log(cmd, "CWD before: " + hdfs.getWorkingDirectory());
            log(cmd, "Requested CWD: " + dir);   
            
            Path newPath = null;
            if(dir.startsWith("/")){
                newPath = new Path(env.getProperty(HDFS_URL), dir);
            } else{
                newPath = new Path(hdfs.getWorkingDirectory(), dir);
            }

            Path qPath = newPath.makeQualified(hdfs);
            System.out.println(newPath);
            if (hdfs.getFileStatus(qPath).isDir() && hdfs.exists(qPath)) {
                hdfs.setWorkingDirectory(qPath);
            }
            else {
                System.out.println("No such directory: " + dir);
            }
            
        }
        catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env);
    }
    
    
    
}
