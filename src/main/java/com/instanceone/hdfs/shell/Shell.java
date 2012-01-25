// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell;

import java.util.ArrayList;
import java.util.Arrays;

import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.instanceone.hdfs.shell.command.Env;
import com.instanceone.hdfs.shell.command.Exit;
import com.instanceone.hdfs.shell.command.HdfsCd;
import com.instanceone.hdfs.shell.command.HdfsConnect;
import com.instanceone.hdfs.shell.command.HdfsHead;
import com.instanceone.hdfs.shell.command.HdfsLs;
import com.instanceone.hdfs.shell.command.HdfsPut;
import com.instanceone.hdfs.shell.command.HdfsPwd;
import com.instanceone.hdfs.shell.command.HdfsRm;
import com.instanceone.hdfs.shell.command.Help;
import com.instanceone.hdfs.shell.command.LocalCd;
import com.instanceone.hdfs.shell.command.LocalLs;
import com.instanceone.hdfs.shell.command.LocalPwd;

public class Shell {
    private static CommandLineParser parser = new PosixParser();

    public static void main(String[] args) throws Exception {
        Environment env = new Environment();
        env.addCommand(new Exit("exit"));
        env.addCommand(new LocalLs("lls", env));
        env.addCommand(new LocalPwd("lpwd"));
        env.addCommand(new LocalCd("lcd", env));
        env.addCommand(new HdfsLs("ls"));
        env.addCommand(new HdfsCd("cd", env));
        env.addCommand(new HdfsPwd("pwd"));
        env.addCommand(new HdfsPut("put", env));
        env.addCommand(new HdfsHead("head", env, false));
        env.addCommand(new HdfsHead("lhead", env, true));
        env.addCommand(new HdfsRm("rm"));
        env.addCommand(new Env("env"));
        env.addCommand(new HdfsConnect("connect"));
        env.addCommand(new Help("help", env));
        

        // create completers
        ArrayList<Completer> completers = new ArrayList<Completer>();
        for (String cmdName : env.commandList()) {
            // command name
            StringsCompleter sc = new StringsCompleter(cmdName);
            
            
            ArrayList<Completer> cmdCompleters = new ArrayList<Completer>();
            // add a completer for the command name
            cmdCompleters.add(sc);
            // add the completer for the command
            cmdCompleters.add(env.getCommand(cmdName).getCompleter());
            // add a terminator for the command
            //cmdCompleters.add(new NullCompleter());
            
            ArgumentCompleter ac = new ArgumentCompleter(cmdCompleters);
            completers.add(ac);
        }

        AggregateCompleter aggComp = new AggregateCompleter(completers);

        ConsoleReader reader = new ConsoleReader();
        reader.addCompleter(aggComp);

        String line;

        while ((line = reader.readLine("hdfs-cli$ ")) != null) {
            String[] argv = line.split("\\s");
            String cmdName = argv[0];

            Command command = env.getCommand(cmdName);
            if (command != null) {
                System.out.println("Running: " + command.getName() + " ("
                                + command.getClass().getName() + ")");
                String[] cmdArgs = Arrays.copyOfRange(argv, 1, argv.length);
                CommandLine cl = parse(command, cmdArgs);
                if (cl != null) {
                    try {
                        command.execute(env, cl, reader);
                    }
                    catch (Throwable e) {
                        System.out.println("Command failed with error: " + e.getMessage());
                        if(cl.hasOption("v")){
                            e.printStackTrace();
                        }
                    }
                }

            }
            else {
                if(cmdName != null && cmdName.length() > 0){
                    System.out.println(cmdName + ": command not found");
                }
            }
        }
    }

    private static CommandLine parse(Command cmd, String[] args) {
        Options opts = cmd.getOptions();
        CommandLine retval = null;
        try {
            retval = parser.parse(opts, args);
        }
        catch (ParseException e) {
            System.err.println(e.getMessage());
        }
        return retval;
    }

}
