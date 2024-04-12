
/*
 * Copyright (c) 2022-2024. David W. Streever All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.cloudera.utils.hadoop.cli;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

//@Component
@Slf4j
@Getter
@Setter
public class HadoopSession extends Shell {

    private String gatewayProxyURL = null;
    private String altEndpoint = null;

//    private Options getOptions() {
//        // create Options object
//        Options options = new Options();
//
//        // add i option
//        Option initOption = new Option("i", "init", true, "Initialization with Set");
//        initOption.setRequired(false);
//        // Commons-Cli v1.3+ (can use currently because of Hadoop Commons-cli version is at 1.2.
//        //        Option initOption = Option.builder("i").required(false)
//        //                .argName("init set").desc("Initialize with set")
//        //                .longOpt("init")
//        //                .hasArg(true).numberOfArgs(1)
//        //                .build();
//        options.addOption(initOption);
//
//        Option executeOption = new Option("e", "execute", true, "Execute Command");
//        executeOption.setRequired(false);
//        //        Option executeOption = Option.builder("e").required(false)
//        //                .argName("command [args]").desc("Execute Command")
//        //                .longOpt("execute")
//        //                .hasArg(true).numberOfArgs(1)
//        //                .build();
//        options.addOption(executeOption);
//
//        // add f option
//        Option fileOption = new Option("f", "file", true, "File to execute");
//        fileOption.setRequired(false);
//        //        Option fileOption = Option.builder("f").required(false)
//        //                .argName("file to exec").desc("Run File and Exit")
//        //                .longOpt("file")
//        //                .hasArg(true).numberOfArgs(1)
//        //                .build();
//        options.addOption(fileOption);
//
//        Option templateOption = new Option("t", "template", true,
//                "Template to apply on input (-f | -stdin)");
//        templateOption.setRequired(false);
//        //        Option templateOption = Option.builder("t").required(false)
//        //                .argName("template").desc("Template to apply on input (-f | -stdin)")
//        //                .longOpt("template")
//        //                .hasArg(true).numberOfArgs(1)
//        //                .build();
//        options.addOption(templateOption);
//
//        Option delimiterOption = new Option("td", "template-delimiter", true,
//                "Delimiter to apply to 'input' for template option (default=',')");
//        delimiterOption.setRequired(false);
//        //        Option delimiterOption = Option.builder("td").required(false)
//        //                .argName("template-delimiter").desc("Delimiter to apply to 'input' for template option (default=',')")
//        //                .longOpt("template-delimiter")
//        //                .hasArg(true).numberOfArgs(1)
//        //                .build();
//        options.addOption(delimiterOption);
//
//        // add stdin option
//        Option siOption = new Option("stdin", "stdin", false, "Run Stdin pipe and Exit");
//        siOption.setRequired(false);
//        //        Option siOption = Option.builder("stdin").required(false)
//        //                .argName("stdin process").desc("Run Stdin pipe and Exit")
//        //                .longOpt("stdin")
//        //                .hasArg(false)
//        //                .build();
//        options.addOption(siOption);
//
//        Option silentOption = new Option("s", "silent", false, "Suppress Banner");
//        silentOption.setRequired(false);
//        //        Option silentOption = Option.builder("s").required(false)
//        //                .argName("silent").desc("Suppress Banner")
//        //                .longOpt("silent")
//        //                .hasArg(false)
//        //                .build();
//        options.addOption(silentOption);
//
//        Option apiOption = new Option("api", "api", false, "API mode");
//        apiOption.setRequired(false);
//        //        Option apiOption = Option.builder("api").required(false)
//        //                .argName("api").desc("API mode")
//        //                .longOpt("api")
//        //                .hasArg(false)
//        //                .build();
//        options.addOption(apiOption);
//
//        Option verboseOption = new Option("v", "verbose", false, "Verbose Commands");
//        verboseOption.setRequired(false);
//        //        Option verboseOption = Option.builder("v").required(false)
//        //                .argName("verbose").desc("Verbose Commands")
//        //                .longOpt("verbose")
//        //                .hasArg(false)
//        //                .build();
//        options.addOption(verboseOption);
//
//        Option debugOption = new Option("d", "debug", false, "Debug Commands");
//        debugOption.setRequired(false);
//        //        Option debugOption = Option.builder("d").required(false)
//        //                .argName("debug").desc("Debug Commands")
//        //                .longOpt("debug")
//        //                .hasArg(false)
//        //                .build();
//        options.addOption(debugOption);
//
//        Option envOption = new Option("ef", "env-file", true, "Environment File(java properties format) with a list of key=values");
//        envOption.setRequired(false);
//        //        Option debugOption = Option.builder("d").required(false)
//        //                .argName("debug").desc("Debug Commands")
//        //                .longOpt("debug")
//        //                .hasArg(false)
//        //                .build();
//        options.addOption(envOption);
//
//
////        Option usernameOption = Option.builder("u").required(false)
////                .argName("username").desc("Username to log into gateway")
////                .longOpt("username")
////                .hasArg(true).numberOfArgs(1)
////                .build();
////        options.addOption(usernameOption);
//
////        Option passwordOption = Option.builder("p").required(false)
////                .argName("password").desc("Password")
////                .longOpt("password")
////                .hasArg(true).numberOfArgs(1)
////                .build();
////        options.addOption(passwordOption);
//
////        Option webhdfsOption = Option.builder("w").required(false)
////                .argName("webhdfs://<host>:<port>").desc("Connect via webhdfs")
////                .longOpt("webhdfs")
////                .hasArg(true).numberOfArgs(1)
////                .build();
////        options.addOption(webhdfsOption);
//
//        // Need to add mechanism to use a password from file.
//        // Need to add mechanism to pull username from file.
//        // Need to add mechanism to pull gateway url from file.
//        // Need to save last state to file for sign in (minus password) in next session.
//
//        Option helpOption = new Option("h", "help", false, "Help");
//        helpOption.setRequired(false);
//        //        Option helpOption = Option.builder("h").required(false)
//        //                .longOpt("help")
//        //                .build();
//        options.addOption(helpOption);
//
//        // TODO: Scripting
//        //options.addOption("f", true, "Script file");
//
//        return options;
//
//    }


//    @Override
//    protected boolean preProcessInitializationArguments(String[] arguments) {
//        boolean rtn = Boolean.TRUE;
//        // create Options object
//        Options options = getOptions();
//
//        CommandLineParser parser = new PosixParser();
//        CommandLine cmd = null;
//        try {
//            cmd = parser.parse(options, arguments);
//        } catch (ParseException pe) {
//            HelpFormatter formatter = new HelpFormatter();
//            formatter.printHelp("<app-cmd>", options);
//            System.exit(-1);
//        }
//
//        Environment lclEnv = new Environment();
//
//        setEnv(lclEnv);
//
//        if (cmd.hasOption("s")) {
//            getEnv().setSilent(true);
//        }
//
//        if (cmd.hasOption("api")) {
//            getEnv().setSilent(true);
//            this.setApiMode(true);
//        }
//
//        if (cmd.hasOption("verbose")) {
//            getEnv().setVerbose(Boolean.TRUE);
//        }
//
//        if (cmd.hasOption("debug")) {
//            getEnv().setDebug(Boolean.TRUE);
//        }
//
//        if (cmd.hasOption("env-file")) {
//            String envProps = cmd.getOptionValue("env-file");
//            File envPropsFile = new File(envProps);
//            try {
//                // Resolve SymLink if necessary
//                Path envPropsPath = envPropsFile.toPath();
//                if (Files.isSymbolicLink(envPropsFile.toPath())) {
//                    // Reset path to look for 'real' file.
//                    envPropsPath = envPropsPath.toRealPath();
//                }
//                InputStream inProps = Files.newInputStream(envPropsPath);
//                Properties extProps = new Properties();
//                extProps.load(inProps);
//                getEnv().getProperties().putAll(extProps);
//            } catch (FileNotFoundException e) {
//                System.out.println("Couldn't locate 'env-file' " + envPropsFile);
//                System.out.println("Additional environment vars NOT loaded");
//            } catch (IOException e) {
//                System.out.println("Problems reading 'env-file' " + envPropsFile + " - " + e.getMessage());
//                System.out.println("Additional environment vars NOT loaded");
//            }
//        }
//
//        if (cmd.hasOption("help")) {
//            HelpFormatter formatter = new HelpFormatter();
//            String cmdline = AbstractShell.substituteVariablesFromManifest("hadoopcli <options> \nversion:${HadoopCLI-Version}");
//            formatter.printHelp(100, cmdline, "Hadoop CLI Utility", options,
//                    "\nVisit https://github.com/dstreev/hadoop-cli/blob/main/README.md for detailed docs");
//            System.exit(-1);
//        }
//        return rtn;
//    }

//    @Override
//    protected boolean postProcessInitializationArguments(String[] arguments) {
//        boolean rtn = Boolean.TRUE;
//        // create Options object
//        Options options = getOptions();
//
//        CommandLineParser parser = new PosixParser();
//        CommandLine cmd = null;
//
//        try {
//            cmd = parser.parse(options, arguments);
//        } catch (ParseException pe) {
//            HelpFormatter formatter = new HelpFormatter();
//            formatter.printHelp("hadoopcli", options);
//            System.exit(-1);
//        }
//
//        // This will allow connections through webhdfs.
////        if (cmd.hasOption("w")) {
////            getEnv().getProperties().setProperty(FileSystem.FS_DEFAULT_NAME_KEY, cmd.getOptionValue("w"));
////            getEnv().getProperties().setProperty(Constants.CONNECT_PROTOCOL, Constants.WEBHDFS);
////            getEnv().setDefaultPrompt("webhdfs-cli:$");
////        } else {
////            getEnv().getProperties().setProperty(Constants.CONNECT_PROTOCOL, Constants.HDFS);
////            getEnv().setDefaultPrompt("hdfs-cli:$");
////        }
//
//
//        if (!autoConnect()) {
//            loge(getEnv(), "Failed to Connect");
//            rtn = Boolean.FALSE;
//        }
//
//        if (cmd.hasOption("i")) {
//            runFile(cmd.getOptionValue("i"), null, null);
//        }
//
//        if (cmd.hasOption("e")) {
//            CommandReturn cr = processInput(cmd.getOptionValue("e"));
//            if (!cr.isError()) {
//                if ( cr.getReturn() != null) {
//                    log(getEnv(), ANSI_GREEN + cr.getReturn() + ANSI_RESET);
//                }
//            } else {
//                loge(getEnv(), ANSI_RESET + "ERROR CODE : " + ANSI_RED + cr.getCode());
//                loge(getEnv(), ANSI_RESET + "   Command : " + ANSI_RED + cr.getCommand());
//                loge(getEnv(), ANSI_RESET + "     ERROR : " + ANSI_RED + cr.getError() + ANSI_RESET);
//            }
//            processInput("exit");
//        }
//        String template = null;
//        String delimiter = null;
//        if (cmd.hasOption("t")) {
//            template = cmd.getOptionValue("t");
//        }
//        if (cmd.hasOption("td")) {
//            delimiter = cmd.getOptionValue("td");
//        }
//
//        if (cmd.hasOption("f")) {
//            runFile(cmd.getOptionValue("f"), template, delimiter);
//            processInput("exit");
//        }
//
//        if (cmd.hasOption("stdin")) {
//            try {
//                File temp = File.createTempFile("hcli", "txt");
//                BufferedWriter tempFileWriter = new BufferedWriter(new FileWriter(temp));
//
//                try (InputStreamReader isr = new InputStreamReader(System.in)) {
//                    int ch;
//                    while ((ch = isr.read()) != -1)
//                        tempFileWriter.write(ch);
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                tempFileWriter.close();
//
//                runFile(temp.getAbsolutePath(), template, delimiter);
//                processInput("exit");
//
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//        }
//        return rtn;
//    }

    // TODO:
//    public void runFile(String inSet, String template, String delimiter) {
//        logv(getEnv(), "-- Running source file: " + inSet);
//
//        String localFile = null;
//
//        // Absolute Path
//        if (inSet.startsWith("/")) {
//            localFile = inSet;
//        } else {
//            // Relative Path
//            org.apache.hadoop.fs.FileSystem localfs = getEnv().getFileSystemOrganizer().getLocalFileSystem();
//
//            String localwd = localfs.getWorkingDirectory().toString();
//
//            if (localwd.split(":").length > 1) {
//                // Remove 'file:' from working directory.
//                localFile = localwd.split(":")[1] + System.getProperty("file.separator") + inSet;
//            } else {
//                localFile = localwd.split(":")[0] + System.getProperty("file.separator") + inSet;
//            }
//        }
//        File setFile = new File(localFile);
//
//        MessageFormat messageFormat = null;
//        if (template != null) {
//            messageFormat = new MessageFormat(template);
//        }
//        String lclDelimiter = null;
//        if (delimiter == null) {
//            lclDelimiter = ",";
//        }
//        if (!setFile.exists()) {
//            loge(getEnv(), "File not found: " + setFile.getAbsoluteFile());
//        } else {
//            try {
//                BufferedReader br = new BufferedReader(new FileReader(setFile));
//                String line = null;
//                int[] status = {0,0};
//
//                log(getEnv(), ANSI_RESET + "[" + ANSI_GREEN + " success " + ANSI_RESET + "/" + ANSI_RED +
//                        " failures " + ANSI_RESET + "] <last command>");
//                log(getEnv(), "[0/0]");
//
//                while ((line = br.readLine()) != null) {
//                    logv(getEnv(), line);
//                    String line2 = line.trim();
//                    if (line2.length() > 0 && !line2.startsWith("#")) {
//                        if (messageFormat != null) {
//                            String[] items = line2.split(lclDelimiter);
//                            line2 = messageFormat.format(items);
//                        }
//                        CommandReturn cr = processInput(line2);
//                        if (!cr.isError()) {
//                            status[0] += 1;
//                            if ( cr.getReturn() != null) {
//                                log(getEnv(), ANSI_GREEN + cr.getReturn() + ANSI_RESET);
//                                log(getEnv(), "" );
//                            }
//                        } else {
//                            status[1] += 1;
//                            loge(getEnv(), ANSI_RESET + "ERROR CODE : " + ANSI_RED + cr.getCode());
//                            loge(getEnv(), ANSI_RESET + "   Command : " + ANSI_RED + cr.getCommand());
//                            loge(getEnv(), ANSI_RESET + "     ERROR : " + ANSI_RED + cr.getError() + ANSI_RESET);
//                            loge(getEnv(), "");
//                        }
//                        StringBuilder sb = new StringBuilder();
//                        sb.append(RESET_TO_PREVIOUS_LINE);
//                        sb.append(ANSI_RESET + "[" + ANSI_GREEN + Integer.toString(status[0]) + ANSI_RESET + "/" + ANSI_RED +
//                                Integer.toString(status[1]) + ANSI_RESET + "] ");
//                        if (!cr.isError()) {
//                            sb.append(ANSI_RESET + "<" + ANSI_GREEN + line2 +  ANSI_RESET + ">");
//                        } else {
//                            sb.append("<" + ANSI_RED + line2 +  ANSI_RED + ">\n");
//                        }
//                        log(getEnv(), sb.toString());
//                    }
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//
//        }
//
//    }

//    private boolean autoConnect() {
//        CommandReturn crTest = null;
//        boolean rtn = true;
//        try {
//            crTest = processInput("connect");
//            if (crTest.isError()) {
//                rtn = false;
//                loge(getEnvironment(), crTest.getError());
//            }
//            // TODO: If Kerberos enabled, pull this from ticket and use auth_to_local to extract. There maybe a Hadoop command for this.
//            String userName = getEnvironment().getProperties().getProperty(HdfsConnect.CURRENT_USER_PROP, System.getProperty("user.name"));
//            String defaultFS = getEnvironment().getConfig().get(HdfsConnect.DEFAULT_FS);
//            log(getEnvironment(), "Default Filesystem: " + defaultFS);
//            if (!defaultFS.startsWith("file:")) {
//                ExecutorService executor = Executors.newCachedThreadPool();
//                String userBaseDir = getEnvironment().getConfig().get(HdfsConnect.FS_USER_DIR, "/user");
//                String homeDir = userBaseDir + "/" + userName;
//                Callable<Object> task = new Callable<Object>() {
//                    public Object call() {
//                        return processInput("cd " + homeDir);
//                    }
//                };
//                Future<Object> future = executor.submit(task);
//                try {
//                    Object result = future.get(30, TimeUnit.SECONDS);
//                    crTest = (CommandReturn) result;
////                org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(homeDir);
//                } catch (TimeoutException ex) {
//                    loge(getEnvironment(), "Login Timeout.  Check for a valid Kerberos Ticket.");
//                    if (getEnvironment().isApiMode()) {
//                        rtn = Boolean.FALSE;
//                    } else {
//                        processInput("exit");
//                    }
//                    // handle the timeout
//                } catch (InterruptedException e) {
//                    // handle the interrupts
//                } catch (ExecutionException e) {
//                    // handle other exceptions
//                    throw new RuntimeException(e);
//                } finally {
//                    future.cancel(true); // may or may not desire this
//                }
//
//                if (crTest.isError()) {
//                    rtn = false;
//                    loge(getEnvironment(), crTest.getError() + ".\nAttempted to set home directory.  User home directory must exist.\nIf user is 'hdfs', consider using a proxy account for audit purposes.");
//                }
//            } else {
//                throw new RuntimeException("Configs aren't set for DFS.  Check config directory: " +
//                        System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf"));
//            }
//        } catch (Exception e) {
////            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
//        return rtn;
//    }
//
//    @Override
//    public void initialize() throws Exception {
//
//        setBannerResource("/hadoop_banner_0.txt");
//
//        // Moved to Environment Bean.
////        getEnv().addCommand(new HdfsCd("cd", getEnv()));
////        getEnv().addCommand(new HdfsPwd("pwd"));
////
////        // remote local
////        getEnv().addCommand(new HdfsCommand("get", getEnv(), Direction.REMOTE_LOCAL));
////        getEnv().addCommand(new HdfsCommand("copyFromLocal", getEnv(), Direction.LOCAL_REMOTE));
////        // local remote
////        getEnv().addCommand(new HdfsCommand("put", getEnv(), Direction.LOCAL_REMOTE));
////        getEnv().addCommand(new HdfsCommand("copyToLocal", getEnv(), Direction.REMOTE_LOCAL));
////        // src dest
////        getEnv().addCommand(new HdfsCommand("cp", getEnv(), Direction.REMOTE_REMOTE));
////
////        // amend to context path, if present
////        getEnv().addCommand(new HdfsCommand("chown", getEnv(), Direction.NONE, 1));
////        getEnv().addCommand(new HdfsCommand("chmod", getEnv(), Direction.NONE, 1));
////        getEnv().addCommand(new HdfsCommand("chgrp", getEnv(), Direction.NONE, 1));
////
////        getEnv().addCommand(new HdfsAllowSnapshot("allowSnapshot", getEnv(), Direction.NONE, 1, false, true));
////        getEnv().addCommand(new HdfsDisallowSnapshot("disallowSnapshot", getEnv(), Direction.NONE, 1, false, true));
////        getEnv().addCommand(new HdfsLsSnapshottableDir("lsSnapshottableDir", getEnv(), Direction.NONE, 1, false, true));
////
////        getEnv().addCommand(new HdfsCommand("createSnapshot", getEnv()));
////        getEnv().addCommand(new HdfsCommand("deleteSnapshot", getEnv()));
////        getEnv().addCommand(new HdfsCommand("renameSnapshot", getEnv()));
////        getEnv().addCommand(new SnapshotDiff("snapshotDiff", getEnv()));
////
////        getEnv().addCommand(new HdfsCommand("du", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("df", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("dus", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("ls", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("lsr", getEnv(), Direction.NONE));
//////        env.addCommand(new HdfsCommand("find", env, Direction.NONE, 1, false));
////
////        getEnv().addCommand(new HdfsCommand("mkdir", getEnv(), Direction.NONE));
////
////        getEnv().addCommand(new HdfsCommand("count", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("stat", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("tail", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("head", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("touchz", getEnv(), Direction.NONE));
////
////        getEnv().addCommand(new HdfsCommand("rm", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("rmdir", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("mv", getEnv(), Direction.REMOTE_REMOTE));
////        getEnv().addCommand(new HdfsCommand("cat", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("test", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("text", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("touchz", getEnv(), Direction.NONE));
////        getEnv().addCommand(new HdfsCommand("checksum", getEnv(), Direction.NONE));
////
//////        getEnv().addCommand(new HdfsScan("scan", getEnv()));
////
//////        getEnv().addCommand(new HdfsCommand("usage", getEnv()));
////
////        // Security Help
//////        env.addCommand(new HdfsUGI("ugi"));
//////        env.addCommand(new HdfsKrb("krb", env, Direction.NONE, 1));
////
////        // HDFS Utils
////        //env.addCommand(new HdfsRepair("repair", env, Direction.NONE, 2, true, true));
////
////        getEnv().addCommand(new Env("env"));
////        getEnv().addCommand(new HdfsConnect("connect"));
////        getEnv().addCommand(new Help("help", getEnv()));
////        getEnv().addCommand(new HistoryCmd("history"));
////
////        // HDFS Tools
////        getEnv().addCommand(new HdfsLsPlus("lsp", getEnv(), Direction.NONE));
//////        getEnv().addCommand(new HdfsNNStats("nnstat", getEnv(), Direction.NONE));
////
////        getEnv().addCommand(new HdfsSource("source", getEnv(), this));
////
////        // MapReduce Tools
////        // TODO: Add back once the field mappings are completed.
//////        getEnv().addCommand(new JhsStats("jhsstat", getEnv(), Direction.NONE));
////
////        // Yarn Tools
////        getEnv().addCommand(new ContainerStatsCommand("cstat", getEnv(), Direction.NONE));
////        getEnv().addCommand(new SchedulerStatsCommand("sstat", getEnv(), Direction.NONE));
////
////        getEnv().addCommand(new Exit("exit"));
////        getEnv().addCommand(new LocalLs("lls", getEnv()));
////        getEnv().addCommand(new LocalPwd("lpwd"));
////        getEnv().addCommand(new LocalCd("lcd", getEnv()));
////
////        getEnv().addCommand(new LocalHead("lhead", getEnv()));
////        getEnv().addCommand(new LocalCat("lcat", getEnv()));
////        getEnv().addCommand(new LocalMkdir("lmkdir", getEnv()));
////        getEnv().addCommand(new LocalRm("lrm", getEnv()));
////
////        getEnv().addCommand(new Use("use", getEnv()));
////        getEnv().addCommand(new List("list", getEnv()));
////        getEnv().addCommand(new List("nss", getEnv()));
////        getEnv().addCommand(new List("namespaces", getEnv()));
//
//    }
//
//    @Override
//    public String getName() {
//        return "hadoopcli";
//    }

}
