/*
 * Copyright (c) 2024. David W. Streever All Rights Reserved
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
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CliFsShell;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Getter
@Setter
public class CliSession {

    private Configuration hadoopConfig;
    private SessionCredentials credentials;
    private UserGroupInformation ugi;

    private CliFsShell shell;
    private FileSystemOrganizer fileSystemOrganizer;
    private CommandRegistry commandRegistry;

    private boolean verbose = false;
    private boolean debug = false;
    private boolean silent = false;

    private Properties properties = new Properties();
    private CommandLineParser parser = new PosixParser();

    // Private constructor - use builder
    private CliSession() {}

    public static CliSessionBuilder builder() {
        return new CliSessionBuilder();
    }

    public void init() throws IOException {
        this.ugi = credentials.getUGI();

        try {
            ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                this.shell = new CliFsShell(hadoopConfig);
                this.shell.init();
                this.fileSystemOrganizer = new FileSystemOrganizer();
                this.fileSystemOrganizer.init(hadoopConfig);
                return null;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Initialization interrupted", e);
        }
    }

    public CliFsShell getShell() {
        log.debug("Getting Shell");
        return shell;
    }

    public FileSystemOrganizer getFileSystemOrganizer() {
        log.debug("Getting FileSystemOrganizer");
        if (fileSystemOrganizer != null) {
            log.trace("Current FileSystem {}: ", fileSystemOrganizer.getCurrentFileSystemState().getFileSystem().getUri());
            log.trace("Current FileSystem Working Directory: {}", fileSystemOrganizer.getCurrentFileSystemState().getWorkingDirectory());
        }
        return fileSystemOrganizer;
    }

    public String getPrompt() {
        return getFileSystemOrganizer().getPrompt();
    }

    public Command getCommand(String name) {
        return commandRegistry.getCommand(name);
    }

    public java.util.Set<String> commandList() {
        return commandRegistry.commandList();
    }

    public CommandReturn processInput(String line) throws DisabledException {
        String adjustedLine = substituteVariables(line + " ");

        // Pipelining support
        String splitRegEx = "\\|(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)";
        String[] cmds = adjustedLine.split(splitRegEx);
        if (cmds.length > 2) {
            CommandReturn crLength = new CommandReturn(CommandReturn.BAD);
            crLength.getErr().print("Only support single depth pipeline at this time.");
            return crLength;
        }

        CommandReturn previousCR = null;
        for (String command : cmds) {
            if (previousCR == null) {
                previousCR = processCommand(command, null);
            } else {
                BufferedReader bufferedReader = new BufferedReader(new StringReader(previousCR.getReturn()));
                CommandReturn innerCR = new CommandReturn(CommandReturn.GOOD);
                String pipedLine;
                while (true) {
                    try {
                        if ((pipedLine = bufferedReader.readLine()) == null) break;
                    } catch (IOException e) {
                        break;
                    }
                    String adjustedPipedLine = pipedLine.contains(" ") ? "\"" + pipedLine + "\"" : pipedLine;
                    String pipedCommand = command.trim() + " " + adjustedPipedLine;
                    innerCR = processCommand(pipedCommand, innerCR);
                }
                previousCR = innerCR;
            }
        }
        return previousCR;
    }

    public CommandReturn processCommand(String line, CommandReturn commandReturn) throws DisabledException {
        try {
            return ugi.doAs((PrivilegedExceptionAction<CommandReturn>) () ->
                processCommandInternal(line, commandReturn));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DisabledException("Command interrupted");
        } catch (IOException e) {
            throw new DisabledException("IO error: " + e.getMessage());
        }
    }

    private CommandReturn processCommandInternal(String line, CommandReturn commandReturn) {
        CommandReturn cr = commandReturn != null ? commandReturn : new CommandReturn(CommandReturn.GOOD);

        List<String> matchList = parseArguments(line);
        if (matchList.isEmpty()) {
            cr.setCode(AbstractCommand.CODE_CMD_ERROR);
            cr.getErr().print("Match List is Empty");
            return cr;
        }

        String cmdName = matchList.get(0);
        Command command = commandRegistry.getCommand(cmdName);

        if (command != null) {
            command.setErr(cr.getErr());
            command.setOut(cr.getOut());

            String[] cmdArgs = matchList.size() > 1
                ? matchList.subList(1, matchList.size()).toArray(new String[0])
                : null;
            CommandLine cl = parse(command, cmdArgs);
            if (cl != null) {
                try {
                    cr = command.execute(this, cl, cr);
                } catch (Throwable e) {
                    log.error("Command failed with error: {}", e.getMessage());
                }
            }
        } else {
            if (cmdName != null && !cmdName.isEmpty()) {
                log.error("{} : command not found", cmdName);
            }
        }
        return cr;
    }

    private List<String> parseArguments(String line) {
        List<String> matchList = new ArrayList<>();
        Pattern regex = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'");
        Matcher regexMatcher = regex.matcher(line);
        while (regexMatcher.find()) {
            if (regexMatcher.group(1) != null) {
                matchList.add(regexMatcher.group(1));
            } else if (regexMatcher.group(2) != null) {
                matchList.add(regexMatcher.group(2));
            } else {
                matchList.add(regexMatcher.group());
            }
        }
        return matchList;
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

    public String substituteVariables(String template) {
        String workingTemplate = template;
        String[] matcherStrings = {"\\$\\{(.+?)\\}", "\\$(.+?)([\\s|\\/])"};
        for (String matcherPattern : matcherStrings) {
            StringBuffer buffer = new StringBuffer();
            Pattern pattern = Pattern.compile(matcherPattern);
            Matcher matcher = pattern.matcher(workingTemplate);
            boolean found = false;
            while (matcher.find()) {
                found = true;
                String matchStr = matcher.group(1);
                try {
                    String replacement = System.getProperty(matchStr);
                    if (replacement == null) {
                        replacement = getProperties().getProperty(matchStr);
                    }
                    if (replacement != null) {
                        matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
                        if (matcher.group(2) != null)
                            buffer.append(matcher.group(2));
                    }
                } catch (IllegalArgumentException iae) {
                    // Silently continue
                }
            }
            if (found) {
                matcher.appendTail(buffer);
                workingTemplate = buffer.toString();
            }
        }
        return workingTemplate;
    }

    public void runFile(String inSet, String template, String delimiter) throws DisabledException {
        log.info("-- Running source file: " + inSet);

        String localFile = null;
        if (inSet.startsWith("/")) {
            localFile = inSet;
        } else {
            org.apache.hadoop.fs.FileSystem localfs = getFileSystemOrganizer().getLocalFileSystem();
            String localwd = localfs.getWorkingDirectory().toString();
            if (localwd.split(":").length > 1) {
                localFile = localwd.split(":")[1] + System.getProperty("file.separator") + inSet;
            } else {
                localFile = localwd.split(":")[0] + System.getProperty("file.separator") + inSet;
            }
        }

        java.io.File setFile = new java.io.File(localFile);
        MessageFormat messageFormat = template != null ? new MessageFormat(template) : null;
        String lclDelimiter = delimiter != null ? delimiter : ",";

        if (!setFile.exists()) {
            log.warn("File not found: " + setFile.getAbsoluteFile());
        } else {
            try {
                BufferedReader br = new BufferedReader(new java.io.FileReader(setFile));
                String fileLine;
                while ((fileLine = br.readLine()) != null) {
                    log.debug("Running: {}", fileLine);
                    String line2 = fileLine.trim();
                    if (!line2.isEmpty() && !line2.startsWith("#")) {
                        if (messageFormat != null) {
                            String[] items = line2.split(lclDelimiter);
                            line2 = messageFormat.format(items);
                        }
                        CommandReturn cr = processInput(line2);
                        if (cr.isError()) {
                            log.warn("Error executing: {}", line2);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Error running file: {}", inSet);
            }
        }
    }

    /**
     * Builder for CliSession
     */
    public static class CliSessionBuilder {
        private Configuration configuration;
        private SessionCredentials credentials = new DefaultCredentials();
        private CommandRegistry commandRegistry;
        private boolean verbose = false;
        private boolean debug = false;
        private boolean silent = false;
        private Properties properties = new Properties();

        public CliSessionBuilder withConfiguration(Configuration config) {
            this.configuration = config;
            return this;
        }

        public CliSessionBuilder withCredentials(SessionCredentials credentials) {
            this.credentials = credentials;
            return this;
        }

        public CliSessionBuilder withCommandRegistry(CommandRegistry registry) {
            this.commandRegistry = registry;
            return this;
        }

        public CliSessionBuilder withVerbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        public CliSessionBuilder withDebug(boolean debug) {
            this.debug = debug;
            return this;
        }

        public CliSessionBuilder withSilent(boolean silent) {
            this.silent = silent;
            return this;
        }

        public CliSessionBuilder withProperties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public CliSession build() throws IOException {
            if (configuration == null) {
                throw new IllegalStateException("Configuration is required");
            }
            if (commandRegistry == null) {
                commandRegistry = new CommandRegistry();
            }

            CliSession session = new CliSession();
            session.setHadoopConfig(configuration);
            session.setCredentials(credentials);
            session.setCommandRegistry(commandRegistry);
            session.setVerbose(verbose);
            session.setDebug(debug);
            session.setSilent(silent);
            session.setProperties(properties);
            session.init();

            return session;
        }
    }
}
