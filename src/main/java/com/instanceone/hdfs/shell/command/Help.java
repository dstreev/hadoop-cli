// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import jline.console.ConsoleReader;
import jline.console.completer.StringsCompleter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;

import com.instanceone.hdfs.shell.AbstractCommand;
import com.instanceone.hdfs.shell.Command;
import com.instanceone.hdfs.shell.Environment;

public class Help extends AbstractCommand {
    private Environment env;

    public Help(String name, Environment env) {
        super(name);
        this.env = env;
        
        StringsCompleter completer = new StringsCompleter(this.env.commandList());
        this.completer = completer;
//        super.getCompleters().add(completer);
        
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
