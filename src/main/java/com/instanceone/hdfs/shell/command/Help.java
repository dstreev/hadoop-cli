// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
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
        
        StringsCompleter strCompleter = new StringsCompleter(this.env.commandList());
        NullCompleter nullCompleter = new NullCompleter();
        Completer completer = new AggregateCompleter(strCompleter, nullCompleter);
        
        this.completer = completer;
        
    }
    

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        if (cmd.getArgs().length == 0) {
            for (String str : env.commandList()) {
                log(cmd, str);
            }
        } else {
            Command command = env.getCommand(cmd.getArgs()[0]);
            logv(cmd, "Get Help for command: " + command.getName() + "(" + command.getClass().getName() + ")");
            printHelp(command);
        }

    }
    
    private void printHelp(Command cmd){
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp(cmd.getUsage(), cmd.getHelpHeader(), cmd.getOptions(), cmd.gethelpFooter());
    }
}
