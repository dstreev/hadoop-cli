// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import java.util.List;

import jline.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;

import com.instanceone.hdfs.shell.AbstractCommand;
import com.instanceone.hdfs.shell.Command;
import com.instanceone.hdfs.shell.Environment;

public class Help extends AbstractCommand {

    public Help(String name) {
        super(name);
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        if (cmd.getArgs().length == 0) {
            for (String str : env.commandList()) {
                System.out.println(str);
            }
        } else {
            Command command = env.getCommand(cmd.getArgs()[0]);
            log(cmd, "Get Help for command: " + command.getName() + "(" + command.getClass().getName() + ")");
            printHelp(command);
        }

    }
    
    private void printHelp(Command cmd){
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp(cmd.getUsage(), cmd.getHelpHeader(), cmd.getOptions(), cmd.gethelpFooter());
    }

}
