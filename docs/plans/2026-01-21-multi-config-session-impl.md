# Multi-Configuration Session Management Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor CliSession/CliEnvironment to support multiple Hadoop configurations with isolated Kerberos credentials, enabling both CLI multi-cluster switching and library usage.

**Architecture:** Commands receive CliSession (execution context) instead of CliEnvironment (registry/factory). SessionCredentials abstraction handles authentication. CommandRegistry extracted as separate class. CliSession gains processCommand and builder pattern.

**Tech Stack:** Java 17, Hadoop 3.x, Spring Boot, Lombok, JLine

---

## Task 1: Create SessionCredentials Interface and Implementations

**Files:**
- Create: `src/main/java/com/cloudera/utils/hadoop/cli/session/SessionCredentials.java`
- Create: `src/main/java/com/cloudera/utils/hadoop/cli/session/DefaultCredentials.java`
- Create: `src/main/java/com/cloudera/utils/hadoop/cli/session/KeytabCredentials.java`
- Create: `src/main/java/com/cloudera/utils/hadoop/cli/session/ProxyCredentials.java`

**Step 1: Create the session package directory**

Run: `mkdir -p src/main/java/com/cloudera/utils/hadoop/cli/session`

**Step 2: Create SessionCredentials interface**

```java
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

package com.cloudera.utils.hadoop.cli.session;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Abstraction for session credentials, allowing different authentication methods.
 */
public interface SessionCredentials {
    /**
     * Get the UserGroupInformation for this credential set.
     * May perform login on first call.
     */
    UserGroupInformation getUGI() throws IOException;

    /**
     * Refresh credentials if needed (e.g., renew Kerberos ticket).
     */
    void refresh() throws IOException;
}
```

**Step 3: Create DefaultCredentials implementation**

```java
/*
 * Copyright (c) 2024. David W. Streever All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  ...
 */

package com.cloudera.utils.hadoop.cli.session;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Default credentials using the current system UGI.
 * Used for CLI app with system Kerberos credentials.
 */
public class DefaultCredentials implements SessionCredentials {
    @Override
    public UserGroupInformation getUGI() throws IOException {
        return UserGroupInformation.getCurrentUser();
    }

    @Override
    public void refresh() throws IOException {
        UserGroupInformation.getCurrentUser().checkTGTAndReloginFromKeytab();
    }
}
```

**Step 4: Create KeytabCredentials implementation**

```java
/*
 * Copyright (c) 2024. David W. Streever All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  ...
 */

package com.cloudera.utils.hadoop.cli.session;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Credentials from a keytab file with isolated UGI.
 * Used for library usage with isolated Kerberos credentials per session.
 */
@Slf4j
public class KeytabCredentials implements SessionCredentials {
    private final String principal;
    private final String keytabPath;
    private UserGroupInformation ugi;

    public KeytabCredentials(String principal, String keytabPath) {
        this.principal = principal;
        this.keytabPath = keytabPath;
    }

    @Override
    public synchronized UserGroupInformation getUGI() throws IOException {
        if (ugi == null) {
            log.info("Logging in from keytab: {} as {}", keytabPath, principal);
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
        }
        return ugi;
    }

    @Override
    public synchronized void refresh() throws IOException {
        if (ugi != null) {
            ugi.checkTGTAndReloginFromKeytab();
        }
    }
}
```

**Step 5: Create ProxyCredentials implementation**

```java
/*
 * Copyright (c) 2024. David W. Streever All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  ...
 */

package com.cloudera.utils.hadoop.cli.session;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * Proxy credentials for impersonation scenarios.
 */
@Slf4j
public class ProxyCredentials implements SessionCredentials {
    private final String proxyUser;
    private final SessionCredentials realCredentials;
    private UserGroupInformation proxyUgi;

    public ProxyCredentials(String proxyUser, SessionCredentials realCredentials) {
        this.proxyUser = proxyUser;
        this.realCredentials = realCredentials;
    }

    @Override
    public synchronized UserGroupInformation getUGI() throws IOException {
        if (proxyUgi == null) {
            log.info("Creating proxy user: {}", proxyUser);
            proxyUgi = UserGroupInformation.createProxyUser(proxyUser, realCredentials.getUGI());
        }
        return proxyUgi;
    }

    @Override
    public void refresh() throws IOException {
        realCredentials.refresh();
    }
}
```

**Step 6: Compile to verify**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

**Step 7: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/session/
git commit -m "feat: add SessionCredentials abstraction for multi-config support

Add SessionCredentials interface with three implementations:
- DefaultCredentials: uses system UGI
- KeytabCredentials: isolated keytab-based login
- ProxyCredentials: impersonation support

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Create CommandRegistry Class

**Files:**
- Create: `src/main/java/com/cloudera/utils/hadoop/cli/CommandRegistry.java`

**Step 1: Create CommandRegistry**

```java
/*
 * Copyright (c) 2024. David W. Streever All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  ...
 */

package com.cloudera.utils.hadoop.cli;

import com.cloudera.utils.hadoop.shell.command.Command;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Registry for CLI commands. Extracted from CliEnvironment to allow
 * shared or per-session command sets.
 */
public class CommandRegistry {
    private final Map<String, Command> commands = new TreeMap<>();

    public void register(Command cmd) {
        commands.put(cmd.getName(), cmd);
    }

    public Command getCommand(String name) {
        return commands.get(name);
    }

    public Set<String> commandList() {
        return commands.keySet();
    }

    public Map<String, Command> getCommands() {
        return commands;
    }
}
```

**Step 2: Compile to verify**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/CommandRegistry.java
git commit -m "feat: extract CommandRegistry from CliEnvironment

Separate command registration into its own class for cleaner
separation of concerns and to support shared/per-session registries.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Update Command Interface

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/shell/command/Command.java`

**Step 1: Update Command interface to use CliSession**

Change the execute and implementation method signatures from `CliEnvironment` to `CliSession`:

```java
package com.cloudera.utils.hadoop.shell.command;

import com.cloudera.utils.hadoop.cli.CliSession;
import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.PrintStream;

public interface Command {

    String getDescription();
    String getHelpHeader();
    String getHelpFooter();
    String getUsage();

    String getName();

    void setErr(PrintStream err);
    void setOut(PrintStream out);

    CommandReturn execute(CliSession session, CommandLine cmd, CommandReturn commandReturn);

    Options getOptions();

    Completer getCompleter();

    CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn);
}
```

**Step 2: Compile (will fail - expected)**

Run: `mvn compile -q 2>&1 | head -20`
Expected: FAIL with errors about CliSession not found and CliEnvironment references

**Step 3: Commit interface change**

```bash
git add src/main/java/com/cloudera/utils/hadoop/shell/command/Command.java
git commit -m "refactor: update Command interface to use CliSession

Change execute() and implementation() signatures from CliEnvironment
to CliSession as part of multi-config session support.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Expand CliSession with Builder and ProcessCommand

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/cli/CliSession.java`

**Step 1: Rewrite CliSession with full implementation**

Replace the existing CliSession with the expanded version that includes:
- Configuration, credentials, shell, FileSystemOrganizer
- verbose, debug, silent, properties (moved from CliEnvironment)
- processCommand and processInput methods
- Builder pattern
- UGI doAs wrapping

```java
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

import com.cloudera.utils.hadoop.cli.session.CommandRegistry;
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
    private CliSession() {
    }

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
```

**Step 2: Compile (will fail - expected due to downstream changes needed)**

Run: `mvn compile -q 2>&1 | head -30`
Expected: FAIL - commands still reference CliEnvironment

**Step 3: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/CliSession.java
git commit -m "feat: expand CliSession with builder, processCommand, credentials

CliSession now:
- Has builder pattern for construction
- Holds configuration, credentials, shell, FileSystemOrganizer
- Contains verbose/debug/silent/properties (moved from CliEnvironment)
- Implements processCommand/processInput with UGI doAs wrapping
- Supports variable substitution and file running

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Update AbstractCommand

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/shell/command/AbstractCommand.java`

**Step 1: Update AbstractCommand to use CliSession**

Change all references from `CliEnvironment` to `CliSession`:

```java
/*
 * Copyright (c) 2022. David W. Streever All Rights Reserved
 * ...
 */

package com.cloudera.utils.hadoop.shell.command;

import com.cloudera.utils.hadoop.cli.CliSession;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.PrintStream;

@Slf4j
public abstract class AbstractCommand implements Command{
    public static final int CODE_BAD_DATE = -321;
    public static final int CODE_SUCCESS = 0;
    public static final int CODE_LOCAL_FS_ISSUE = -123;
    public static final int CODE_NOT_CONNECTED = -10;
    public static final int CODE_CONNECTION_ISSUE = -11;
    public static final int CODE_CMD_ERROR = -1;
    public static final int CODE_PATH_ERROR = -20;
    public static final int CODE_FS_CLOSE_ISSUE = -100;
    public static final int CODE_STATS_ISSUE = -200;
    public static final int CODE_NOT_FOUND = 1;

    public PrintStream out = System.out;
    public PrintStream err = System.err;
    private final String name;

    protected Completer completer = new NullCompleter();

    public AbstractCommand(String name){
        this.name = name;
    }

    public String getHelpHeader() {
        StringBuilder sb = new StringBuilder();
        sb.append(getDescription()).append("\n");
        sb.append("Options:");
        return sb.toString();
    }

    public String getHelpFooter() {
        return null;
    }

    @Override
    public void setOut(PrintStream out) {
        this.out = out;
    }

    @Override
    public void setErr(PrintStream err) {
        this.err = err;
    }

    public String getName() {
        return name;
    }

    public Options getOptions() {
        Options options = new Options();
        options.addOption("v", "verbose", false, "show verbose output");
        return options;
    }

    protected void processCommandLine(CommandLine commandLine) {
    }

    public String getUsage(){
        return getName() + " [Options ...] [Args ...]";
    }

    protected static void logv(CliSession session, String log){
        if(session.isVerbose()){
            System.out.println(log);
        }
    }

    protected static void log(CliSession session, String log){
        System.out.println(log);
    }

    protected static void logd(CliSession session, String log){
        if(session.isDebug()){
            System.out.println(log);
        }
    }

    public Completer getCompleter() {
        return this.completer;
    }

    @Override
    public CommandReturn execute(CliSession session, CommandLine cmd, CommandReturn cr) {
        CommandReturn lclCr = cr;
        if (lclCr == null) {
            lclCr = new CommandReturn(CommandReturn.GOOD);
        }
        try {
            lclCr = implementation(session, cmd, lclCr);
        } catch (Throwable t) {
            log.error("Error in Command: {}", getName(), t);
        }
        return lclCr;
    }

    @Override
    public abstract CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn);
}
```

**Step 2: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/shell/command/AbstractCommand.java
git commit -m "refactor: update AbstractCommand to use CliSession

Change execute/implementation and logging helpers to use CliSession
instead of CliEnvironment.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Update HdfsAbstract and PathBuilder

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/hdfs/shell/command/HdfsAbstract.java`
- Modify: `src/main/java/com/cloudera/utils/hadoop/hdfs/shell/command/PathBuilder.java`

**Step 1: Update PathBuilder to use CliSession**

```java
package com.cloudera.utils.hadoop.hdfs.shell.command;

import com.cloudera.utils.hadoop.hdfs.util.FileSystemOrganizer;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.cli.CliSession;

public class PathBuilder {
    // ... SupportedProtocol enum stays the same ...

    private final CliSession session;
    private final PathDirectives directives;

    public PathBuilder(CliSession session, PathDirectives directives) {
        this.session = session;
        this.directives = directives;
    }

    public PathBuilder(CliSession session) {
        this.session = session;
        this.directives = new PathDirectives();
    }

    // ... static methods stay the same ...

    public String buildPath(Side side, String[] args) {
        String rtn = null;
        FileSystemOrganizer fso = session.getFileSystemOrganizer();
        FileSystemState lfss = fso.getFileSystemState(Constants.LOCAL_FS);
        FileSystemState fss = fso.getCurrentFileSystemState();
        // ... rest of method stays the same ...
    }
}
```

**Step 2: Update HdfsAbstract**

Remove the `env` field, update constructors, and create PathBuilder at execution time:

```java
package com.cloudera.utils.hadoop.hdfs.shell.command;

import com.cloudera.utils.hadoop.cli.CliSession;
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public abstract class HdfsAbstract extends AbstractCommand {

    protected PathDirectives pathDirectives;

    public HdfsAbstract(String name) {
        super(name);
    }

    public HdfsAbstract(String name, Direction directionContext) {
        super(name);
        pathDirectives = new PathDirectives(directionContext);
    }

    public HdfsAbstract(String name, Direction directionContext, int directives) {
        super(name);
        pathDirectives = new PathDirectives(directionContext, directives);
    }

    public HdfsAbstract(String name, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name);
        pathDirectives = new PathDirectives(directionContext, directives, directivesBefore, directivesOptional);
    }

    protected PathBuilder getPathBuilder(CliSession session) {
        if (pathDirectives != null) {
            return new PathBuilder(session, pathDirectives);
        } else {
            return new PathBuilder(session);
        }
    }

    @Override
    public Options getOptions() {
        return super.getOptions();
    }

    protected void processCommandLine(CommandLine commandLine) {
        super.processCommandLine(commandLine);
    }
}
```

**Step 3: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/hdfs/shell/command/HdfsAbstract.java
git add src/main/java/com/cloudera/utils/hadoop/hdfs/shell/command/PathBuilder.java
git commit -m "refactor: update HdfsAbstract and PathBuilder for CliSession

- Remove env field from HdfsAbstract
- Update constructors to not require CliEnvironment
- PathBuilder now created at execution time via getPathBuilder(session)
- PathBuilder accepts CliSession instead of CliEnvironment

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 7: Update All Command Implementations

**Files to modify (all in `src/main/java/com/cloudera/utils/hadoop/`):**
- `hdfs/shell/command/HdfsCommand.java`
- `hdfs/shell/command/HdfsCd.java`
- `hdfs/shell/command/HdfsPwd.java`
- `hdfs/shell/command/HdfsConnect.java`
- `hdfs/shell/command/HdfsAllowSnapshot.java`
- `hdfs/shell/command/HdfsDisallowSnapshot.java`
- `hdfs/shell/command/HdfsLsSnapshottableDir.java`
- `hdfs/shell/command/SnapshotDiff.java`
- `hdfs/shell/command/Use.java`
- `hdfs/shell/command/List.java`
- `hdfs/shell/command/LocalCat.java`
- `hdfs/shell/command/LocalCd.java`
- `hdfs/shell/command/LocalHead.java`
- `hdfs/shell/command/LocalLs.java`
- `hdfs/shell/command/LocalMkdir.java`
- `hdfs/shell/command/LocalPwd.java`
- `hdfs/shell/command/LocalRm.java`
- `hdfs/util/HdfsLsPlus.java`
- `hdfs/util/HdfsSource.java`
- `AbstractStats.java`
- `yarn/ContainerStatsCommand.java`
- `yarn/SchedulerStatsCommand.java`
- `shell/commands/Help.java`
- `shell/commands/Exit.java`
- `shell/commands/Env.java`
- `shell/commands/HistoryCmd.java`

For each file, the pattern is:
1. Change import from `CliEnvironment` to `CliSession`
2. Update constructor to remove `CliEnvironment` parameter
3. Update `implementation()` signature from `CliEnvironment env` to `CliSession session`
4. Replace `env.` calls with `session.` calls
5. Use `getPathBuilder(session)` instead of `pathBuilder` field

**Example: HdfsCommand.java**

```java
// Change imports
import com.cloudera.utils.hadoop.cli.CliSession;

// Update constructors - remove CliEnvironment parameter
public HdfsCommand(String name, Direction directionContext) {
    super(name, directionContext);
}

// Update implementation signature
public CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn cr) {
    CliFsShell shell = session.getShell();
    // ...
    FileSystemOrganizer fso = session.getFileSystemOrganizer();
    PathBuilder pathBuilder = getPathBuilder(session);
    // ... rest uses pathBuilder and session instead of env
}

// Update getCompleter to not use env field
@Override
public Completer getCompleter() {
    // Completers need redesign - return NullCompleter for now
    return new NullCompleter();
}
```

**Step 1-22: Update each command file following the pattern above**

This is a mechanical refactoring. For each file:
- Replace `CliEnvironment` with `CliSession` in imports
- Remove `CliEnvironment` from constructor parameters
- Change `implementation(CliEnvironment env, ...)` to `implementation(CliSession session, ...)`
- Replace `env.` with `session.`
- Replace `pathBuilder.` with `getPathBuilder(session).` or create local variable

**Step 23: Compile to verify all commands updated**

Run: `mvn compile -q`
Expected: May still fail due to completers and other references

**Step 24: Commit command updates**

```bash
git add src/main/java/com/cloudera/utils/hadoop/
git commit -m "refactor: update all command implementations to use CliSession

Update 25+ command classes to:
- Accept CliSession instead of CliEnvironment
- Remove env field from constructors
- Use session.getFileSystemOrganizer(), session.getShell(), etc.
- Create PathBuilder at execution time

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 8: Update FileSystemNameCompleter and Other Completers

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/hdfs/shell/completers/FileSystemNameCompleter.java`
- Modify: `src/main/java/com/cloudera/utils/hadoop/hdfs/shell/completers/NamespaceCompleter.java`

**Step 1: Update FileSystemNameCompleter**

The completer needs access to a session. For now, we can make it accept a session supplier or defer initialization:

```java
package com.cloudera.utils.hadoop.hdfs.shell.completers;

import com.cloudera.utils.hadoop.cli.CliSession;
// ... rest of imports

public class FileSystemNameCompleter implements Completer {
    private final java.util.function.Supplier<CliSession> sessionSupplier;
    private boolean local = false;

    public FileSystemNameCompleter(java.util.function.Supplier<CliSession> sessionSupplier) {
        this.sessionSupplier = sessionSupplier;
    }

    public FileSystemNameCompleter(java.util.function.Supplier<CliSession> sessionSupplier, boolean local) {
        this.sessionSupplier = sessionSupplier;
        this.local = local;
    }

    // In complete() method, get session from supplier
    public int complete(String buffer, final int cursor, final List<CharSequence> candidates) {
        CliSession session = sessionSupplier.get();
        if (session == null) return 0;

        // Replace env. with session.
        // ...
    }
}
```

**Step 2: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/hdfs/shell/completers/
git commit -m "refactor: update completers to use CliSession supplier

Completers now accept a Supplier<CliSession> to get the current
session at completion time, supporting multi-session scenarios.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 9: Update CliEnvironment as Factory/Registry

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java`

**Step 1: Refactor CliEnvironment**

CliEnvironment becomes a factory and registry for sessions:

```java
package com.cloudera.utils.hadoop.cli;

import com.cloudera.utils.hadoop.cli.session.CommandRegistry;
import com.cloudera.utils.hadoop.cli.session.DefaultCredentials;
import com.cloudera.utils.hadoop.cli.session.SessionCredentials;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemOrganizer;
import com.cloudera.utils.hadoop.shell.command.Command;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import jline.console.ConsoleReader;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.cloudera.utils.hadoop.hdfs.shell.command.HdfsConnect.*;

@Component
@Slf4j
@Getter
@Setter
public class CliEnvironment {

    public static final String RESET_TO_PREVIOUS_LINE = "\33[1A\33[2K";
    public static final String ANSI_RESET = "\u001B[0m";
    // ... other ANSI constants ...

    private String defaultPrompt = "basic:$";
    private String currentPrompt = null;

    private boolean verbose = false;
    private boolean debug = false;
    private boolean silent = false;
    private boolean apiMode = false;
    private boolean disabled = false;
    private boolean initialized = false;
    private String template = null;
    private String templateDelimiter = ",";

    private Configuration defaultHadoopConfig = null;
    private ConsoleReader consoleReader = null;
    private Properties properties = new Properties();

    // Session management
    private Map<String, CliSession> sessions = new ConcurrentHashMap<>();
    private CommandRegistry defaultRegistry = new CommandRegistry();
    private String defaultSessionName = "default";
    private String currentSessionName = "default";

    public synchronized void init() {
        if (!isDisabled() && !isInitialized()) {
            try {
                log.info("Initializing Hadoop Configuration");
                defaultHadoopConfig = loadSystemConfiguration();

                // Setup Kerberos if needed
                if (defaultHadoopConfig.get("hadoop.security.authentication", "simple").equalsIgnoreCase("kerberos")) {
                    UserGroupInformation.setConfiguration(defaultHadoopConfig);
                    getProperties().setProperty(CURRENT_USER_PROP, UserGroupInformation.getCurrentUser().getShortUserName());
                }

                // Create default session
                createSession(defaultSessionName, defaultHadoopConfig, new DefaultCredentials());

                setInitialized(true);
            } catch (IOException e) {
                log.error("Failed to initialize: {}", e.getMessage());
            }
        }
    }

    private Configuration loadSystemConfiguration() {
        String hadoopConfDirProp = System.getenv().getOrDefault(HADOOP_CONF_DIR, "/etc/hadoop/conf");
        Configuration config = new Configuration(true);

        File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
        for (String file : HADOOP_CONF_FILES) {
            File f = new File(hadoopConfDir, file);
            if (f.exists()) {
                config.addResource(new org.apache.hadoop.fs.Path(f.getAbsolutePath()));
            }
        }

        getProperties().stringPropertyNames().forEach(k -> {
            config.set(k, getProperties().getProperty(k));
        });

        return config;
    }

    public CliSession createSession(String name, Configuration config) throws IOException {
        return createSession(name, config, new DefaultCredentials());
    }

    public CliSession createSession(String name, Configuration config, SessionCredentials credentials) throws IOException {
        CliSession session = CliSession.builder()
                .withConfiguration(config)
                .withCredentials(credentials)
                .withCommandRegistry(defaultRegistry)
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

    // Command registration delegates to registry
    public void addCommand(Command cmd) {
        defaultRegistry.register(cmd);
    }

    public Command getCommand(String name) {
        return defaultRegistry.getCommand(name);
    }

    public Set<String> commandList() {
        return defaultRegistry.commandList();
    }

    // Delegate to current session
    public CommandReturn processInput(String line) throws DisabledException {
        if (disabled) {
            throw new DisabledException("CLI Environment is disabled.");
        }
        return getCurrentSession().processInput(line);
    }

    public void runFile(String inSet, String template, String delimiter) throws DisabledException {
        getCurrentSession().runFile(inSet, template, delimiter);
    }

    // For backwards compatibility and shell access
    public CliFsShell getShell() {
        return getCurrentSession().getShell();
    }

    public FileSystemOrganizer getFileSystemOrganizer() {
        return getCurrentSession().getFileSystemOrganizer();
    }

    public String getPrompt() {
        return getCurrentSession().getPrompt();
    }

    public Configuration getHadoopConfig() {
        return getCurrentSession().getHadoopConfig();
    }
}
```

**Step 2: Compile**

Run: `mvn compile -q`

**Step 3: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java
git commit -m "refactor: CliEnvironment as factory/registry for sessions

CliEnvironment now:
- Manages named sessions in ConcurrentHashMap
- Acts as factory via createSession()
- Delegates to CommandRegistry for command management
- Delegates processInput/runFile to current session
- Maintains backwards compatibility for shell access

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 10: Update HadoopCliAppCfg for New Command Registration

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/cli/HadoopCliAppCfg.java`

**Step 1: Update command registration**

Commands no longer take CliEnvironment in constructor. Update all `new XxxCommand("name", cliEnvironment, ...)` to `new XxxCommand("name", ...)`:

```java
@Bean
@Order(5)
CommandLineRunner initCommands(CliEnvironment cliEnvironment) {
    return args -> {
        cliEnvironment.addCommand(new HdfsCd("cd"));
        cliEnvironment.addCommand(new HdfsPwd("pwd"));

        cliEnvironment.addCommand(new HdfsCommand("get", Direction.REMOTE_LOCAL));
        cliEnvironment.addCommand(new HdfsCommand("copyFromLocal", Direction.LOCAL_REMOTE));
        cliEnvironment.addCommand(new HdfsCommand("put", Direction.LOCAL_REMOTE));
        // ... update all command registrations to remove cliEnvironment parameter

        cliEnvironment.addCommand(new Help("help", cliEnvironment.getDefaultRegistry()));
        // Help needs registry for listing commands
    };
}
```

**Step 2: Update Help command to accept CommandRegistry**

The Help command needs access to command list. Update it to accept CommandRegistry:

```java
public class Help extends AbstractCommand {
    private final CommandRegistry registry;

    public Help(String name, CommandRegistry registry) {
        super(name);
        this.registry = registry;
    }

    @Override
    public CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn) {
        // Use registry.commandList() and registry.getCommand()
    }
}
```

**Step 3: Compile and test**

Run: `mvn compile -q`

**Step 4: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/HadoopCliAppCfg.java
git add src/main/java/com/cloudera/utils/hadoop/shell/commands/Help.java
git commit -m "refactor: update HadoopCliAppCfg for new command registration

- Commands no longer require CliEnvironment in constructor
- Help command updated to accept CommandRegistry
- All command registrations updated

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 11: Update Tests

**Files:**
- Modify: `src/test/java/com/cloudera/utils/hadoop/api/CliEnvironmentTestcase01.java`
- Modify: `src/test/java/com/cloudera/utils/hadoop/api/CliEnvironmentTestcase02.java`

**Step 1: Update test classes**

Tests should still work as CliEnvironment.processInput delegates to session:

```java
// Tests should work without changes since CliEnvironment.processInput
// delegates to getCurrentSession().processInput()
// Verify the tests still compile and run
```

**Step 2: Run tests**

Run: `mvn test -q`

**Step 3: Commit**

```bash
git add src/test/java/
git commit -m "test: verify tests work with refactored session management

Tests continue to use CliEnvironment.processInput which now
delegates to the current session.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 12: Full Build and Verification

**Step 1: Clean build**

Run: `mvn clean compile -q`
Expected: BUILD SUCCESS

**Step 2: Run all tests**

Run: `mvn test`
Expected: Tests pass (some may be skipped if they require Hadoop cluster)

**Step 3: Package**

Run: `mvn package -DskipTests -q`
Expected: BUILD SUCCESS

**Step 4: Final commit**

```bash
git add -A
git commit -m "chore: complete multi-config session management refactoring

Full refactoring complete:
- SessionCredentials abstraction with Default/Keytab/Proxy implementations
- CommandRegistry extracted for shared command management
- CliSession expanded with builder, processCommand, credentials support
- All commands updated to use CliSession instead of CliEnvironment
- CliEnvironment now acts as factory/registry with named session support
- Backwards compatibility maintained for existing CLI usage

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Summary

Total tasks: 12
Files created: 5 (SessionCredentials, DefaultCredentials, KeytabCredentials, ProxyCredentials, CommandRegistry)
Files modified: ~30 (Command interface, AbstractCommand, HdfsAbstract, PathBuilder, CliSession, CliEnvironment, HadoopCliAppCfg, all command implementations, completers, tests)

Key architectural changes:
1. Commands receive `CliSession` instead of `CliEnvironment`
2. `CliSession` now contains processCommand logic with UGI doAs wrapping
3. `SessionCredentials` abstraction enables isolated Kerberos credentials
4. `CliEnvironment` becomes a factory/registry managing named sessions
5. `CommandRegistry` extracted for cleaner separation
