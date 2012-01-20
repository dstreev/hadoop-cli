// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import jline.ConsoleReader;

import org.apache.commons.cli.CommandLine;

import com.instanceone.hdfs.shell.AbstractCommand;
import com.instanceone.hdfs.shell.Environment;

public class LocalPwd extends AbstractCommand {

    public LocalPwd(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        String cwd = env.getProperty(Environment.CWD);
        if(cwd == null){
            cwd = System.getProperty("user.dir");
            env.setProperty(Environment.CWD, cwd);
        }
        System.out.println(cwd);
    }

}
