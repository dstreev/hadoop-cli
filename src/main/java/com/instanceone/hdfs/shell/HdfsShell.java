// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell;

import com.instanceone.hdfs.shell.command.*;
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

    private boolean kerberos = false;
    private String nnPrin = "nn";
    private String nnHost = "_HOST";
    private String realm = "EXAMPLE.COM";

    public static void main(String[] args) throws Exception {
        new HdfsShell().run(args);
    }

    private Options getOptions() {
        // create Options object
        Options options = new Options();

        // Checking for Kerberos Init.
        Option kerbOption = Option.builder("k").required(false)
                .argName("REALM[,Namenode Prin][,NN Host]")
                .desc("Enable Kerberos Connections")
                .hasArg(true)
                .numberOfArgs(Option.UNLIMITED_VALUES)
                .valueSeparator(',')
                .longOpt("kerberos")
                .build();
        options.addOption(kerbOption);

        // add i option
        Option initOption = Option.builder("i").required(false)
                .argName("init set").desc("Initialize with set")
                .longOpt("init")
                .hasArg(true).numberOfArgs(1)
                .build();
        options.addOption(initOption);

        Option autoOption = Option.builder("a").required(false)
                .argName("auto-connect").desc("Auto Connect")
                .longOpt("auto")
                .hasArg(false)
                .build();
        options.addOption(autoOption);

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

        if (cmd.hasOption("kerberos")) {
            kerberos = true;

//            namenodePrincipal = cmd.getOptionValue("kerberos");
            String[] nnKerberosInfo = cmd.getOptionValues("kerberos");
            realm = nnKerberosInfo[0];
            if (nnKerberosInfo.length > 1)
                nnPrin = nnKerberosInfo[1];
            if (nnKerberosInfo.length > 2)
                nnHost = nnKerberosInfo[2];

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

        if (cmd.hasOption("init")) {
            initialSet(cmd.getOptionValue("init"), reader);
        }

        if (cmd.hasOption("auto")) {
            autoConnect(reader);
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

        if (kerberos) {
            env.setProperty(HdfsKrb.USE_KERBEROS, "true");
            String nnPrincipal = nnPrin + "/" + nnHost + "@" + realm;
            env.setProperty(HdfsKrb.HADOOP_KERBEROS_NN_PRINCIPAL, nnPrincipal);

            System.out.print("Using Kerberos. ");
            System.out.println("  Namenode Principal: " + nnPrincipal);

        }

        env.addCommand(new Exit("exit"));
        env.addCommand(new LocalLs("lls", env));
        env.addCommand(new LocalPwd("lpwd"));
        env.addCommand(new LocalCd("lcd", env));
        env.addCommand(new HdfsCd("cd", env));
        env.addCommand(new HdfsPwd("pwd"));

        // remote local
        env.addCommand(new HdfsCommand("get", env, HdfsCommand.Direction.REMOTE_LOCAL));
        env.addCommand(new HdfsCommand("copyFromLocal", env, HdfsCommand.Direction.LOCAL_REMOTE));
        // local remote
        env.addCommand(new HdfsCommand("put", env, HdfsCommand.Direction.LOCAL_REMOTE));
        env.addCommand(new HdfsCommand("copyToLocal", env, HdfsCommand.Direction.REMOTE_LOCAL));
        // src dest
        env.addCommand(new HdfsCommand("cp", env, HdfsCommand.Direction.REMOTE_REMOTE));

        // amend to context path, if present
        env.addCommand(new HdfsCommand("chown", env, HdfsCommand.Direction.NONE, 1));
        env.addCommand(new HdfsCommand("chmod", env, HdfsCommand.Direction.NONE, 1));
        env.addCommand(new HdfsCommand("chgrp", env, HdfsCommand.Direction.NONE, 1));

        env.addCommand(new HdfsCommand("createSnapshot", env, HdfsCommand.Direction.NONE, 1, false, true));
        env.addCommand(new HdfsCommand("deleteSnapshot", env, HdfsCommand.Direction.NONE, 1, false, false));
        env.addCommand(new HdfsCommand("renameSnapshot", env, HdfsCommand.Direction.NONE, 2, false, false));

        env.addCommand(new HdfsCommand("du", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("df", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("dus", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("ls", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("lsr", env, HdfsCommand.Direction.NONE));
//        env.addCommand(new HdfsCommand("find", env, HdfsCommand.Direction.NONE, 1, false));


        env.addCommand(new HdfsCommand("mkdir", env, HdfsCommand.Direction.NONE));

        env.addCommand(new HdfsCommand("count", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("stat", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("tail", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("head", env, HdfsCommand.Direction.NONE));
//        env.addCommand(new HdfsCommand("test", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("touchz", env, HdfsCommand.Direction.NONE));

        env.addCommand(new HdfsCommand("rm", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("rmdir", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("mv", env, HdfsCommand.Direction.REMOTE_REMOTE));
        env.addCommand(new HdfsCommand("cat", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("text", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("checksum", env, HdfsCommand.Direction.NONE));
        env.addCommand(new HdfsCommand("usage", env));

        // Security Help
        env.addCommand(new HdfsUGI("ugi"));
        env.addCommand(new HdfsKrb("krb", env, HdfsCommand.Direction.NONE, 1));


        env.addCommand(new LocalHead("lhead", env, true));
        env.addCommand(new LocalCat("lcat", env, true));
        env.addCommand(new LocalMkdir("lmkdir", env, true));
        env.addCommand(new LocalRm("lrm", true));
        env.addCommand(new Env("env"));
        env.addCommand(new HdfsConnect("connect"));
        env.addCommand(new Help("help", env));
        env.addCommand(new HistoryCmd("history"));

    }

    @Override
    public String getName() {
        return "hdfs-cli";
    }

}
