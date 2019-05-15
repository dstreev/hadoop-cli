// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.streever.hadoop;

import com.streever.hadoop.hdfs.shell.command.*;
import com.streever.hadoop.hdfs.util.HdfsLsPlus;
import com.streever.hadoop.hdfs.util.HdfsNNStats;
import com.streever.hadoop.hdfs.util.HdfsSource;
import com.streever.hadoop.mapreduce.JhsStats;
import com.streever.hadoop.yarn.ContainerStats;
import com.streever.hadoop.yarn.SchedulerStats;
import com.streever.hadoop.hdfs.shell.command.LocalCat;
import com.streever.hadoop.hdfs.shell.command.LocalHead;
import com.streever.tools.stemshell.BasicEnvironmentImpl;
import com.streever.tools.stemshell.Environment;
import com.streever.tools.stemshell.commands.Env;
import com.streever.tools.stemshell.commands.Exit;
import com.streever.tools.stemshell.commands.Help;
import com.streever.tools.stemshell.commands.HistoryCmd;
import jline.console.ConsoleReader;
import org.apache.commons.cli.*;

import java.io.*;

public class HadoopShell extends com.streever.tools.stemshell.AbstractShell {

    private enum Mode { CLI, PROXY }

    private Mode state = Mode.CLI;
    private String gatewayProxyURL = null;
    
    public static void main(String[] args) throws Exception {
        new HadoopShell().run(args);
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

        // add f option
        Option runOption = Option.builder("r").required(false)
                .argName("run").desc("Run File and Exit")
                .longOpt("run file")
                .hasArg(true).numberOfArgs(1)
                .build();
        options.addOption(runOption);

        // add stdin option
        Option siOption = Option.builder("stdin").required(false)
                .argName("stdin process").desc("Run Stdin pipe and Exit")
                .longOpt("stdin")
                .hasArg(false)
                .build();
        options.addOption(siOption);

        Option gatewayOption = Option.builder("g").required(false)
                .argName("gateway").desc("Use Gateway(Knox Proxy)")
                .longOpt("gateway")
                .hasArg(true).numberOfArgs(1)
                .build();
        options.addOption(gatewayOption);

        Option verboseOption = Option.builder("v").required(false)
                .argName("verbose").desc("Verbose Commands")
                .longOpt("verbose")
                .hasArg(false)
                .build();
        options.addOption(verboseOption);

        Option debugOption = Option.builder("d").required(false)
                .argName("debug").desc("Debug Commands")
                .longOpt("debug")
                .hasArg(false)
                .build();
        options.addOption(debugOption);

        Option usernameOption = Option.builder("u").required(false)
                .argName("username").desc("Username to log into gateway")
                .longOpt("username")
                .hasArg(true).numberOfArgs(1)
                .build();
        options.addOption(usernameOption);

        Option passwordOption = Option.builder("p").required(false)
                .argName("password").desc("Password")
                .longOpt("password")
                .hasArg(true).numberOfArgs(1)
                .build();
        options.addOption(passwordOption);

        // Need to add mechanism to use a password from file.
        // Need to add mechanism to pull username from file.
        // Need to add mechanism to pull gateway url from file.
        // Need to save last state to file for sign in (minus password) in next session.

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
            formatter.printHelp("hadoop-cli", options);
        }

        if (cmd.hasOption("gateway")) {
            state = Mode.PROXY;
            gatewayProxyURL = cmd.getOptionValue("gateway");
            System.out.println("Using Gateway Proxy: " + gatewayProxyURL);
        }

        Environment lclEnv = new BasicEnvironmentImpl();

        setEnv(lclEnv);

        switch (state) {
            case CLI:
                getEnv().setDefaultPrompt("hdfs-cli:$");
                break;
            case PROXY:
                getEnv().setDefaultPrompt("hdfs-proxy-cli:$");
                break;
            default:
        }


        if (cmd.hasOption("verbose")) {
            getEnv().setVerbose(Boolean.TRUE);
        }

        if (cmd.hasOption("debug")) {
            getEnv().setDebug(Boolean.TRUE);
        }

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("hadoop-cli", options);
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
            formatter.printHelp("hadoop-cli", options);
        }

        autoConnect(reader);

        if (cmd.hasOption("i")) {
            runFile(cmd.getOptionValue("i"), reader);
        }

        if (cmd.hasOption("r")) {
            runFile(cmd.getOptionValue("r"), reader);
            processInput("exit", reader);
        }

        if (cmd.hasOption("stdin")) {
            try {
                File temp = File.createTempFile("hcli", "txt");
                BufferedWriter tempFileWriter = new BufferedWriter(new FileWriter(temp));

                try (InputStreamReader isr = new InputStreamReader(System.in)) {
                    int ch;
                    while ((ch = isr.read()) != -1)
                        tempFileWriter.write(ch);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                tempFileWriter.close();

                runFile(temp.getAbsolutePath(), reader);
                processInput("exit", reader);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    public void runFile(String set, ConsoleReader reader) {
        logv(getEnv(),"-- Running source file: " + set);

        String localFile = null;
        
        if (set.startsWith("/")) {
            localFile = set;
        } else {
            org.apache.hadoop.fs.FileSystem localfs = (org.apache.hadoop.fs.FileSystem) getEnv().getValue(Constants.LOCAL_FS);
//        org.apache.hadoop.fs.FileSystem hdfs = (org.apache.hadoop.fs.FileSystem) getEnv().getValue(Constants.HDFS);

            String localwd = localfs.getWorkingDirectory().toString();
//        String hdfswd = hdfs.getWorkingDirectory().toString();

            // Remove 'file:' from working directory.
            localFile = localwd.split(":")[1] + System.getProperty("file.separator") + set;
        }
        File setFile = new File(localFile);
        
        if (!setFile.exists()) {
            loge(getEnv(), "File not found: " + setFile.getAbsoluteFile());
        } else {
            try {
                BufferedReader br = new BufferedReader(new FileReader(setFile));
                String line = null;
                while ((line = br.readLine()) != null) {
                    log(getEnv(), line);
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

    @Override
    public void initialize() throws Exception {

        setBannerResource("/hadoop_banner.txt");
        
        switch (state) {
            case PROXY:

            break;
            case CLI:
                
                getEnv().addCommand(new HdfsCd("cd", getEnv()));
                getEnv().addCommand(new HdfsPwd("pwd"));

                // remote local
                getEnv().addCommand(new HdfsCommand("get", getEnv(), Direction.REMOTE_LOCAL));
                getEnv().addCommand(new HdfsCommand("copyFromLocal", getEnv(), Direction.LOCAL_REMOTE));
                // local remote
                getEnv().addCommand(new HdfsCommand("put", getEnv(), Direction.LOCAL_REMOTE));
                getEnv().addCommand(new HdfsCommand("copyToLocal", getEnv(), Direction.REMOTE_LOCAL));
                // src dest
                getEnv().addCommand(new HdfsCommand("cp", getEnv(), Direction.REMOTE_REMOTE));

                // amend to context path, if present
                getEnv().addCommand(new HdfsCommand("chown", getEnv(), Direction.NONE, 1));
                getEnv().addCommand(new HdfsCommand("chmod", getEnv(), Direction.NONE, 1));
                getEnv().addCommand(new HdfsCommand("chgrp", getEnv(), Direction.NONE, 1));

                getEnv().addCommand(new HdfsCommand("createSnapshot", getEnv(), Direction.NONE, 1, false, true));
                getEnv().addCommand(new HdfsCommand("deleteSnapshot", getEnv(), Direction.NONE, 1, false, false));
                getEnv().addCommand(new HdfsCommand("renameSnapshot", getEnv(), Direction.NONE, 2, false, false));

                getEnv().addCommand(new HdfsCommand("du", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("df", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("dus", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("ls", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("lsr", getEnv(), Direction.NONE));
//        env.addCommand(new HdfsCommand("find", env, Direction.NONE, 1, false));


                getEnv().addCommand(new HdfsCommand("mkdir", getEnv(), Direction.NONE));

                getEnv().addCommand(new HdfsCommand("count", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("stat", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("tail", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("head", getEnv(), Direction.NONE));
//        env.addCommand(new HdfsCommand("test", env, Direction.NONE));
                getEnv().addCommand(new HdfsCommand("touchz", getEnv(), Direction.NONE));

                getEnv().addCommand(new HdfsCommand("rm", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("rmdir", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("mv", getEnv(), Direction.REMOTE_REMOTE));
                getEnv().addCommand(new HdfsCommand("cat", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("test", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("text", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("touch", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("checksum", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsCommand("usage", getEnv()));

                // Security Help
//        env.addCommand(new HdfsUGI("ugi"));
//        env.addCommand(new HdfsKrb("krb", env, Direction.NONE, 1));

                // HDFS Utils
                //env.addCommand(new HdfsRepair("repair", env, Direction.NONE, 2, true, true));

                getEnv().addCommand(new Env("env"));
                getEnv().addCommand(new HdfsConnect("connect"));
                getEnv().addCommand(new Help("help", getEnv()));
                getEnv().addCommand(new HistoryCmd("history"));

                // HDFS Tools
                getEnv().addCommand(new HdfsLsPlus("lsp", getEnv(), Direction.NONE));
                getEnv().addCommand(new HdfsNNStats("nnstat", getEnv(), Direction.NONE));

                getEnv().addCommand(new HdfsSource("source", getEnv(), this));

                // MapReduce Tools
                getEnv().addCommand(new JhsStats("jhsstat", getEnv(), Direction.NONE));

                // Yarn Tools
                getEnv().addCommand(new ContainerStats("cstat", getEnv(), Direction.NONE));
                getEnv().addCommand(new SchedulerStats("sstat", getEnv(), Direction.NONE));

            break;

            default:

        }
        getEnv().addCommand(new Exit("exit"));
        getEnv().addCommand(new LocalLs("lls", getEnv()));
        getEnv().addCommand(new LocalPwd("lpwd"));
        getEnv().addCommand(new LocalCd("lcd", getEnv()));

        getEnv().addCommand(new LocalHead("lhead", getEnv(), true));
        getEnv().addCommand(new LocalCat("lcat", getEnv(), true));
        getEnv().addCommand(new LocalMkdir("lmkdir", getEnv(), true));
        getEnv().addCommand(new LocalRm("lrm", true));

    }

    @Override
    public String getName() {
        return "hadoop-cli";
    }

}
