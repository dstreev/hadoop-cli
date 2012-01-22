// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import java.io.IOException;
import java.util.Date;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.Environment;

public class HdfsLs extends HdfsCommand {

    public HdfsLs(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            FileSystem hdfs = super.getFileSystem(env, reader);
            String cwd = super.cwd(env, reader);
            
//            String lsFile = ;

            Path srcPath = cmd.getArgs().length == 0 ? new Path(cwd) : new Path(cwd, cmd.getArgs()[0]);
            FileStatus[] files = hdfs.listStatus(srcPath);
            for (FileStatus file : files) {
                // String fileName = file.getPath().
                if (cmd.hasOption("l")) {
                    System.out.println(
                                    (file.isDir() ? "d" : "-")+
                                    file.getPermission() + "\t"
                                    + file.getOwner() + ":" + file.getGroup()
                                    + "\t" + file.getLen() 
                                    + "\t" + new Date(file.getModificationTime())
                    
                                    + "\t" + file.getPath().getName());
                }
                else {
                    System.out.println(file.getPath().getName());
                }
            }
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    @Override
    public Options getOptions() {
        // TODO Auto-generated method stub
        Options opts = super.getOptions();
        opts.addOption("l", false, "show extended file attributes");
        return opts;
    }

}
