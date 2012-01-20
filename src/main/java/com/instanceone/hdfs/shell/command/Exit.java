// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import jline.ConsoleReader;

import org.apache.commons.cli.CommandLine;

import com.instanceone.hdfs.shell.AbstractCommand;
import com.instanceone.hdfs.shell.Environment;

public class Exit extends AbstractCommand {

    public Exit(String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        System.exit(0);
    }

}
