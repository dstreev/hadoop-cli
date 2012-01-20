// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell;

import jline.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public interface Command {

    String getHelpHeader();
    String gethelpFooter();
    String getUsage();
    
    String getName();
    
    void execute(Environment env, CommandLine cmd, ConsoleReader reader);
    
    Options getOptions();
}
