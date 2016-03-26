// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell;

import com.dstreev.hadoop.util.HdfsLsPlus;
import com.dstreev.hadoop.util.HdfsNNStats;
import com.dstreev.hdfs.shell.command.Direction;
import com.dstreev.hdfs.shell.command.LocalMkdir;
import com.dstreev.hdfs.shell.command.LocalRm;
import com.instanceone.hdfs.shell.command.*;
import com.dstreev.hadoop.util.HdfsRepair;
import com.instanceone.hdfs.shell.command.LocalCat;
import com.instanceone.hdfs.shell.command.LocalHead;
import com.instanceone.stemshell.Environment;
import com.instanceone.stemshell.commands.Env;
import com.instanceone.stemshell.commands.Exit;
import com.instanceone.stemshell.commands.Help;
import com.instanceone.stemshell.commands.HistoryCmd;
import jline.console.ConsoleReader;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class HdfsShell extends com.instanceone.stemshell.Shell {

    public static void main(String[] args) throws Exception {
        new HdfsShell().run(args);
    }

    private Options getOptions() {
        // create Options object
        Options options = new Options();

        // add i option
        Option initOption = Option.builder("i").required(false)
                .argName("init set").desc("Initialize with set")
                .longOpt("init")
                .hasArg(true).numberOfArgs(1)
                .build();
        options.addOption(initOption);

        Option helpOption = Option.builder("?").required(false)
                .longOpt("help")
                .build();
        options.addOption(helpOption);

        // TODO: Scripting
        //options.addOption("f", true, "Script file");

        return options;

    }

    @Override
    protected void preProcessInitializationArguments(String[] arguments) {
        super.preProcessInitializationArguments(arguments);

        // create Options object
        Options options = getOptions();

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, arguments);
        } catch (ParseException pe) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("hdfs-cli", options);
        }

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("hdfs-cli", options);
            System.exit(-1);
        }

    }

    @Override
    protected void postProcessInitializationArguments(String[] arguments, ConsoleReader reader) {
        super.postProcessInitializationArguments(arguments, reader);

        // create Options object
        Options options = getOptions();

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, arguments);
        } catch (ParseException pe) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("hdfs-cli", options);
        }

        autoConnect(reader);

        if (cmd.hasOption("init")) {
            initialSet(cmd.getOptionValue("init"), reader);
        }

    }

    private void initialSet(String set, ConsoleReader reader) {
        System.out.println("-- Initializing with set: " + set);

        File dir = new File(System.getProperty("user.home"), "."
                + this.getName());
        if (dir.exists() && dir.isFile()) {
            throw new IllegalStateException(
                    "Default configuration file exists and is not a directory: "
                            + dir.getAbsolutePath());
        } else if (!dir.exists()) {
            dir.mkdir();
        }
        // directory created, touch history file
        File setFile = new File(dir, set);
        if (!setFile.exists()) {
            try {
                if (!setFile.createNewFile()) {
                    throw new IllegalStateException(
                            "Unable to create set file: "
                                    + setFile.getAbsolutePath());
                } else {
                    System.out.println("New Initialization File create in: " + System.getProperty("user.home") + System.getProperty("file.separator") + this.getName() + System.getProperty("file.separator") + set + ". Add commands to this file to initialized the next session");
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        } else {
            try {
                BufferedReader br = new BufferedReader(new FileReader(setFile));
                String line = null;
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                    String line2 = line.trim();
                    if (line2.length() > 0 && !line2.startsWith("#")) {
                        processInput(line2, reader);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    private void autoConnect(ConsoleReader reader) {
        try {
            String userHome = System.getProperty("user.name");
            processInput("connect", reader);
            processInput("cd /user/" + userHome, reader);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void runScript(String file, ConsoleReader reader) {

    }

    @Override
    public void initialize(Environment env) throws Exception {

        env.addCommand(new Exit("exit"));
        env.addCommand(new LocalLs("lls", env));
        env.addCommand(new LocalPwd("lpwd"));
        env.addCommand(new LocalCd("lcd", env));
        env.addCommand(new HdfsCd("cd", env));
        env.addCommand(new HdfsPwd("pwd"));

        // remote local
        env.addCommand(new HdfsCommand("get", env, Direction.REMOTE_LOCAL));
        env.addCommand(new HdfsCommand("copyFromLocal", env, Direction.LOCAL_REMOTE));
        // local remote
        env.addCommand(new HdfsCommand("put", env, Direction.LOCAL_REMOTE));
        env.addCommand(new HdfsCommand("copyToLocal", env, Direction.REMOTE_LOCAL));
        // src dest
        env.addCommand(new HdfsCommand("cp", env, Direction.REMOTE_REMOTE));

        // amend to context path, if present
        env.addCommand(new HdfsCommand("chown", env, Direction.NONE, 1));
        env.addCommand(new HdfsCommand("chmod", env, Direction.NONE, 1));
        env.addCommand(new HdfsCommand("chgrp", env, Direction.NONE, 1));

        env.addCommand(new HdfsCommand("createSnapshot", env, Direction.NONE, 1, false, true));
        env.addCommand(new HdfsCommand("deleteSnapshot", env, Direction.NONE, 1, false, false));
        env.addCommand(new HdfsCommand("renameSnapshot", env, Direction.NONE, 2, false, false));

        env.addCommand(new HdfsCommand("du", env, Direction.NONE));
        env.addCommand(new HdfsCommand("df", env, Direction.NONE));
        env.addCommand(new HdfsCommand("dus", env, Direction.NONE));
        env.addCommand(new HdfsCommand("ls", env, Direction.NONE));
        env.addCommand(new HdfsCommand("lsr", env, Direction.NONE));
//        env.addCommand(new HdfsCommand("find", env, Direction.NONE, 1, false));


        env.addCommand(new HdfsCommand("mkdir", env, Direction.NONE));

        env.addCommand(new HdfsCommand("count", env, Direction.NONE));
        env.addCommand(new HdfsCommand("stat", env, Direction.NONE));
        env.addCommand(new HdfsCommand("tail", env, Direction.NONE));
        env.addCommand(new HdfsCommand("head", env, Direction.NONE));
//        env.addCommand(new HdfsCommand("test", env, Direction.NONE));
        env.addCommand(new HdfsCommand("touchz", env, Direction.NONE));

        env.addCommand(new HdfsCommand("rm", env, Direction.NONE));
        env.addCommand(new HdfsCommand("rmdir", env, Direction.NONE));
        env.addCommand(new HdfsCommand("mv", env, Direction.REMOTE_REMOTE));
        env.addCommand(new HdfsCommand("cat", env, Direction.NONE));
        env.addCommand(new HdfsCommand("text", env, Direction.NONE));
        env.addCommand(new HdfsCommand("checksum", env, Direction.NONE));
        env.addCommand(new HdfsCommand("usage", env));

        // Security Help
//        env.addCommand(new HdfsUGI("ugi"));
//        env.addCommand(new HdfsKrb("krb", env, Direction.NONE, 1));

        // HDFS Utils
        //env.addCommand(new HdfsRepair("repair", env, Direction.NONE, 2, true, true));

        env.addCommand(new LocalHead("lhead", env, true));
        env.addCommand(new LocalCat("lcat", env, true));
        env.addCommand(new LocalMkdir("lmkdir", env, true));
        env.addCommand(new LocalRm("lrm", true));
        env.addCommand(new Env("env"));
        env.addCommand(new HdfsConnect("connect"));
        env.addCommand(new Help("help", env));
        env.addCommand(new HistoryCmd("history"));

        // HDFS Tools
        env.addCommand(new HdfsLsPlus("lsp", env, Direction.NONE));
        env.addCommand(new HdfsNNStats("nnstat", env, Direction.NONE));

    }

    @Override
    public String getName() {
        return "hdfs-cli";
    }

}
