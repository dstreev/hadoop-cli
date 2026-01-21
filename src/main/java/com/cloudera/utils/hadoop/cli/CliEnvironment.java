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

import com.cloudera.utils.hadoop.cli.session.DefaultCredentials;
import com.cloudera.utils.hadoop.cli.session.SessionCredentials;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemOrganizer;
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.shell.command.Command;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import jline.console.ConsoleReader;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CliFsShell;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.stereotype.Component;

import java.io.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.cloudera.utils.hadoop.hdfs.shell.command.HdfsConnect.*;

@Component
@Slf4j
@Getter
@Setter
public class CliEnvironment {

    public static final String RESET_TO_PREVIOUS_LINE = "\33[1A\33[2K";
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";

    private String defaultPrompt = "basic:$";
    private String currentPrompt = null;

    private boolean verbose = Boolean.FALSE;
    private boolean debug = Boolean.FALSE;
    private boolean silent = Boolean.FALSE;
    private boolean apiMode = Boolean.FALSE;
    private boolean disabled = Boolean.FALSE;
    private boolean initialized = Boolean.FALSE;
    private String template = null;
    private String templateDelimiter = ",";

    // Default Hadoop configuration used to create sessions
    private Configuration defaultHadoopConfig = null;

    private ConsoleReader consoleReader = null;

    private Properties properties = new Properties();
    private HashMap<String, Object> values = new HashMap<String, Object>();
    private Map<String, Command> commands = new TreeMap<String, Command>();
    private CommandLineParser parser = new PosixParser();

    // Session management
    private Map<String, CliSession> sessions = new ConcurrentHashMap<>();
    private String defaultSessionName = "default";
    private String currentSessionName = "default";

    private CommandRegistry registry = new CommandRegistry();

    public CliEnvironment(CommandRegistry registry) {
        this.registry = registry;
    }

    public synchronized void init() {
        if (!isDisabled() && !isInitialized()) {
            try {
                log.info("Initializing Hadoop Configuration");
                // Get a value that over rides the default, if nothing then use default.
                String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");

                org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration(true);
                this.defaultHadoopConfig = config;

                File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
                for (String file : HADOOP_CONF_FILES) {
                    File f = new File(hadoopConfDir, file);
                    if (f.exists()) {
                        config.addResource(new org.apache.hadoop.fs.Path(f.getAbsolutePath()));
                    }
                }

                // hadoop.security.authentication
                if (config.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
                    UserGroupInformation.setConfiguration(config);
                    getProperties().setProperty(CURRENT_USER_PROP, UserGroupInformation.getCurrentUser().getShortUserName());
                }

                getProperties().stringPropertyNames().forEach(k -> {
                    config.set(k, getProperties().getProperty(k));
                });

                org.apache.hadoop.fs.FileSystem hdfs = null;
                try {
                    hdfs = org.apache.hadoop.fs.FileSystem.get(config);
                } catch (Throwable t) {
                    log.error("Error connecting to HDFS: {}", t.getMessage());
                }

                // Create default session
                createSession(defaultSessionName, defaultHadoopConfig, new DefaultCredentials());

                setInitialized(Boolean.TRUE);

            } catch (IOException e) {
                log.error(e.getMessage());
            }
        }

    }

    // ============================================
    // Session Management Methods
    // ============================================

    public CliSession createSession(String name, Configuration config) throws IOException {
        return createSession(name, config, new DefaultCredentials());
    }

    public CliSession createSession(String name, Configuration config, SessionCredentials credentials) throws IOException {
        // Attempt to lookup a session by name before creating one.
        CliSession session = CliSession.builder()
            .withConfiguration(config)
            .withCredentials(credentials)
            .withCommandRegistry(registry)
            .withVerbose(verbose)
            .withDebug(debug)
            .withSilent(silent)
            .withProperties(properties)
            .build();
        sessions.put(name, session);
        return session;
    }

    public CliSession getSession(String name) {
        return sessions.get(name);
    }

    public CliSession getDefaultSession() {
        return sessions.get(defaultSessionName);
    }

    public CliSession getCurrentSession() {
        return sessions.get(currentSessionName);
    }

    public void setCurrentSession(String name) {
        if (sessions.containsKey(name)) {
            this.currentSessionName = name;
        }
    }

    public void removeSession(String name) {
        if (!name.equals(defaultSessionName)) {
            sessions.remove(name);
        }
    }

    public Set<String> listSessions() {
        return sessions.keySet();
    }

    public CommandRegistry getDefaultRegistry() {
        return registry;
    }

    // ============================================
    // Backward Compatibility Methods (delegate to current session)
    // ============================================

    public CliFsShell getShell() {
        return getCurrentSession().getShell();
    }

    public FileSystemOrganizer getFileSystemOrganizer() {
        return getCurrentSession().getFileSystemOrganizer();
    }

    public void register(Command cmd) {
        registry.register(cmd);
    }

    public String getPrompt() {
        CliSession session = getCurrentSession();
        return session != null ? session.getPrompt() : defaultPrompt;
    }

    public Configuration getHadoopConfig() {
        CliSession session = getCurrentSession();
        return session != null ? session.getHadoopConfig() : defaultHadoopConfig;
    }

    public Command getCommand(String name) {
        return registry.getCommand(name);
    }

    public Set<String> commandList() {
        return registry.commandList();
    }

    public Object getValue(String key) {
        return this.values.get(key);
    }

    public Boolean isVerbose() {
        return verbose;
    }

    private CommandLine parse(Command cmd, String[] args) {
        Options opts = cmd.getOptions();
        CommandLine retval = null;
        try {
            retval = parser.parse(opts, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
        }
        return retval;
    }

    protected CommandReturn processCommand(String line, CommandReturn commandReturn) throws DisabledException {
        // Deal with args that are in quotes and don't split them.
//        String[] argv = line.split("\\s+(?=((\\\\[\\\\\"]|[^\\\\\"])*\"(\\\\[\\\\\"]|[^\\\\\"])*\")*(\\\\[\\\\\"]|[^\\\\\"])*$)");
//        String[] argv = line.split("[^\\s\\\"']+|\\\"([^\\\"]*)\\\"|'([^']*)'");
//        String cmdName = argv[0];
        CommandReturn cr = commandReturn;
        if (cr == null) {
            cr = new CommandReturn(CommandReturn.GOOD);
        }

        if (disabled) {
            throw new DisabledException("CLI Environment is disabled.");
        }

        // Looking for escape chars and quotes that allow for special chars and spaces
        java.util.List<String> matchList = new ArrayList<String>();
        Pattern regex = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'");
        Matcher regexMatcher = regex.matcher(line);
        while (regexMatcher.find()) {
            if (regexMatcher.group(1) != null) {
                // Add double-quoted string without the quotes
                matchList.add(regexMatcher.group(1));
            } else if (regexMatcher.group(2) != null) {
                // Add single-quoted string without the quotes
                matchList.add(regexMatcher.group(2));
            } else {
                // Add unquoted word
                matchList.add(regexMatcher.group());
            }
        }

        String[] argv = new String[matchList.size()];
        matchList.toArray(argv);


        if (matchList.isEmpty()) {
            cr.setCode(AbstractCommand.CODE_CMD_ERROR);
            cr.getErr().print("Match List is Empty");
            return cr;
        }

        String cmdName = argv[0];

        Command command = getCommand(cmdName);

        if (command != null) {
            // Set Command io.
            command.setErr(cr.getErr());
            command.setOut(cr.getOut());

            String[] cmdArgs = null;
            if (argv.length > 1) {
                cmdArgs = Arrays.copyOfRange(argv, 1, argv.length);
            }
            CommandLine cl = parse(command, cmdArgs);
            if (cl != null) {
                try {
                    cr = command.execute(getCurrentSession(), cl, cr);
                } catch (Throwable e) {
                    log.error("Command failed with error: {}", e.getMessage());
                    // TODO: Does this need to go to the screen?
                } finally {
                }
            }

        } else {
            if (cmdName != null && !cmdName.isEmpty()) {
                log.error("{} : command not found", cmdName);
            }
        }
        return cr;
    }

    public CommandReturn processInput(String line) throws DisabledException {
        if (disabled) {
            throw new DisabledException("CLI Environment is disabled.");
        }
        return getCurrentSession().processInput(line);
    }

    public void runFile(String inSet, String template, String delimiter) throws DisabledException {
        log.info("-- Running source file: " + inSet);

        String localFile = null;

        // Absolute Path
        if (inSet.startsWith("/")) {
            localFile = inSet;
        } else {
            // Relative Path
            org.apache.hadoop.fs.FileSystem localfs = getFileSystemOrganizer().getLocalFileSystem();

            String localwd = localfs.getWorkingDirectory().toString();

            if (localwd.split(":").length > 1) {
                // Remove 'file:' from working directory.
                localFile = localwd.split(":")[1] + System.getProperty("file.separator") + inSet;
            } else {
                localFile = localwd.split(":")[0] + System.getProperty("file.separator") + inSet;
            }
        }
        File setFile = new File(localFile);

        MessageFormat messageFormat = null;
        if (template != null) {
            messageFormat = new MessageFormat(template);
        }
        String lclDelimiter = null;
        if (delimiter == null) {
            lclDelimiter = ",";
        }
        if (!setFile.exists()) {
            log.warn("File not found: " + setFile.getAbsoluteFile());
        } else {
            try {
                BufferedReader br = new BufferedReader(new FileReader(setFile));
                String line = null;
                int[] status = {0,0};

                log.info(ANSI_RESET + "[" + ANSI_GREEN + " success " + ANSI_RESET + "/" + ANSI_RED +
                        " failures " + ANSI_RESET + "] <last command>");
                log.info("[0/0]");

                while ((line = br.readLine()) != null) {
                    log.debug("Running: {}", line);
                    String line2 = line.trim();
                    if (!line2.isEmpty() && !line2.startsWith("#")) {
                        if (messageFormat != null) {
                            String[] items = line2.split(lclDelimiter);
                            line2 = messageFormat.format(items);
                        }
                        CommandReturn cr = processInput(line2);
                        if (!cr.isError()) {
                            status[0] += 1;
                            if ( cr.getReturn() != null) {
                                log.info(ANSI_GREEN + cr.getReturn() + ANSI_RESET);
                                log.info("");
                            }
                        } else {
                            status[1] += 1;
                            log.warn(ANSI_RESET + "ERROR CODE : " + ANSI_RED + cr.getCode());
                            log.warn(ANSI_RESET + "   Command : " + ANSI_RED + cr.getCommand());
                            log.warn(ANSI_RESET + "     ERROR : " + ANSI_RED + cr.getError() + ANSI_RESET);
                            log.warn("");
                        }
                        StringBuilder sb = new StringBuilder();
                        sb.append(RESET_TO_PREVIOUS_LINE);
                        sb.append(ANSI_RESET + "[" + ANSI_GREEN + Integer.toString(status[0]) + ANSI_RESET + "/" + ANSI_RED +
                                Integer.toString(status[1]) + ANSI_RESET + "] ");
                        if (!cr.isError()) {
                            sb.append(ANSI_RESET + "<" + ANSI_GREEN + line2 +  ANSI_RESET + ">");
                        } else {
                            sb.append("<" + ANSI_RED + line2 +  ANSI_RED + ">\n");
                        }
                        log.info(sb.toString());
                    }
                }
            } catch (Exception e) {
                log.error("Error running file: {}", inSet);
            }
        }
    }

    public String substituteVariables(String template) {
        // StringBuilder cannot be used here because Matcher expects StringBuffer
        String workingTemplate = template;
        String matcherStrings[] = {"\\$\\{(.+?)\\}", "\\$(.+?)([\\s|\\/])"};
        for (String matcherPattern : matcherStrings) {
            StringBuffer buffer = new StringBuffer();

            Pattern pattern = Pattern.compile(matcherPattern);
            Matcher matcher = pattern.matcher(workingTemplate);
            boolean found = false;
            while (matcher.find()) {
//                if (found) buffer.append(" ");
                found = true;
                String matchStr = matcher.group(1);
                try {
                    String replacement = null;
                    replacement = System.getProperty(matchStr);
                    if (replacement == null) {
                        replacement = getProperties().getProperty(matchStr);
                    }
//                Manifests.read(matchStr);
                    if (replacement != null) {
                        // quote to work properly with $ and {,} signs
                        matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
                        if (matcher.group(2) != null)
                            buffer.append(matcher.group(2));
                    } else {
//                    System.out.println("No replacement found for: " + matchStr);
                    }
                } catch (IllegalArgumentException iae) {
                    //iae.printStackTrace();
                    // Couldn't locate MANIFEST Entry.
                    // Silently continue. Usually happens in IDE->run.
                }
            }
            if (found) {
                matcher.appendTail(buffer);
                workingTemplate = buffer.toString();
            }
        }

//        String rtn = buffer.toString();
        return workingTemplate;
    }

}
