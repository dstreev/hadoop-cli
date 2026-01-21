# Multi-Configuration Session Management Design

## Overview

Refactor the CLI architecture to support multiple Hadoop configurations and sessions, enabling both interactive multi-cluster switching and library usage for programmatic access to differently configured systems.

## Problem Statement

The current architecture has these limitations:

1. `CliEnvironment` loads configuration from system settings at init time (`HADOOP_CONF_DIR`)
2. `CliSession` is created via `ThreadLocal` and always uses the global `hadoopConfig`
3. Commands are coupled to `CliEnvironment`, receiving it at execution time
4. No way to create sessions with different configurations or credentials
5. Kerberos authentication is tied to the JVM's default credentials

This prevents:
- Connecting to multiple clusters in the same session
- Using the library programmatically with custom configurations
- Isolated Kerberos credentials per connection

## Design Goals

1. Support interactive switching between different Hadoop clusters
2. Enable library usage where external applications create sessions with custom configurations
3. Support isolated Kerberos credentials per session
4. Clean separation of concerns between environment (registry/factory) and session (execution context)
5. Commands should be stateless and reusable across sessions

## Architecture

### Class Responsibilities

| Class | Role |
|-------|------|
| `CliSession` | Execution context: holds config, credentials, shell, FileSystemOrganizer; executes commands |
| `CliEnvironment` | Factory and registry: creates sessions, manages named sessions, holds default config |
| `CommandRegistry` | Command registration and lookup (extracted from CliEnvironment) |
| `SessionCredentials` | Interface for credential providers |
| `Command` | Interface receives `CliSession` instead of `CliEnvironment` |

### Class Diagrams

```
┌─────────────────────────────────────────────────────────────┐
│                      CliEnvironment                          │
├─────────────────────────────────────────────────────────────┤
│ - sessions: Map<String, CliSession>                         │
│ - defaultRegistry: CommandRegistry                          │
│ - defaultHadoopConfig: Configuration                        │
├─────────────────────────────────────────────────────────────┤
│ + init(): void                                              │
│ + createSession(name, config): CliSession                   │
│ + createSession(name, config, credentials): CliSession      │
│ + getSession(name): CliSession                              │
│ + getDefaultSession(): CliSession                           │
│ + processInput(line): CommandReturn  // delegates to default│
└─────────────────────────────────────────────────────────────┘
                              │
                              │ creates/manages
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        CliSession                            │
├─────────────────────────────────────────────────────────────┤
│ - hadoopConfig: Configuration                               │
│ - credentials: SessionCredentials                           │
│ - ugi: UserGroupInformation                                 │
│ - shell: CliFsShell                                         │
│ - fileSystemOrganizer: FileSystemOrganizer                  │
│ - commandRegistry: CommandRegistry                          │
│ - verbose, debug, silent: boolean                           │
│ - properties: Properties                                    │
├─────────────────────────────────────────────────────────────┤
│ + builder(): CliSessionBuilder                              │
│ + init(): void                                              │
│ + processCommand(line, cr): CommandReturn                   │
│ + processInput(line): CommandReturn                         │
│ + substituteVariables(template): String                     │
│ + getFileSystemOrganizer(): FileSystemOrganizer             │
│ + getShell(): CliFsShell                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ uses
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    CommandRegistry                           │
├─────────────────────────────────────────────────────────────┤
│ - commands: Map<String, Command>                            │
├─────────────────────────────────────────────────────────────┤
│ + register(cmd): void                                       │
│ + getCommand(name): Command                                 │
│ + commandList(): Set<String>                                │
└─────────────────────────────────────────────────────────────┘
```

### Credentials Abstraction

```
┌─────────────────────────────────────┐
│      <<interface>>                  │
│      SessionCredentials             │
├─────────────────────────────────────┤
│ + getUGI(): UserGroupInformation    │
│ + refresh(): void                   │
└─────────────────────────────────────┘
            ▲
            │ implements
    ┌───────┼───────────────┐
    │       │               │
┌───┴───┐ ┌─┴─────────┐ ┌───┴──────────┐
│Default│ │  Keytab   │ │    Proxy     │
│Creds  │ │  Creds    │ │    Creds     │
└───────┘ └───────────┘ └──────────────┘
```

**DefaultCredentials**: Uses `UserGroupInformation.getCurrentUser()`. For CLI app with system credentials.

**KeytabCredentials**: Accepts principal and keytab path, calls `UserGroupInformation.loginUserFromKeytabAndReturnUGI()`. For library usage with isolated credentials.

**ProxyCredentials**: Accepts proxy user and real credentials, creates proxy UGI via `UserGroupInformation.createProxyUser()`. For impersonation scenarios.

## Detailed Design

### SessionCredentials Interface

```java
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

### DefaultCredentials

```java
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

### KeytabCredentials

```java
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

### ProxyCredentials

```java
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

### CliSession

```java
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

        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            this.shell = new CliFsShell(hadoopConfig);
            this.shell.init();
            this.fileSystemOrganizer = new FileSystemOrganizer();
            this.fileSystemOrganizer.init(hadoopConfig);
            return null;
        });
    }

    public CommandReturn processInput(String line) throws DisabledException {
        String adjustedLine = substituteVariables(line + " ");
        // Pipelining logic (same as current CliEnvironment)
        // ...
        return processCommand(adjustedLine, null);
    }

    public CommandReturn processCommand(String line, CommandReturn commandReturn) throws DisabledException {
        try {
            return ugi.doAs((PrivilegedExceptionAction<CommandReturn>) () -> {
                return processCommandInternal(line, commandReturn);
            });
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

    public String substituteVariables(String template) {
        // Same logic as current CliEnvironment, using this.properties
    }

    private List<String> parseArguments(String line) {
        // Same regex parsing logic as current CliEnvironment
    }

    private CommandLine parse(Command cmd, String[] args) {
        // Same parsing logic
    }
}
```

### CliSessionBuilder

```java
public class CliSessionBuilder {
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
```

### CommandRegistry

```java
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
}
```

### Updated Command Interface

```java
public interface Command {
    String getName();
    String getDescription();
    String getHelpHeader();
    String getHelpFooter();
    String getUsage();

    void setErr(PrintStream err);
    void setOut(PrintStream out);

    // Changed: CliSession instead of CliEnvironment
    CommandReturn execute(CliSession session, CommandLine cmd, CommandReturn commandReturn);
    CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn);

    Options getOptions();
    Completer getCompleter();
}
```

### Updated CliEnvironment

```java
@Component
@Slf4j
@Getter
@Setter
public class CliEnvironment {
    private Map<String, CliSession> sessions = new ConcurrentHashMap<>();
    private CommandRegistry defaultRegistry = new CommandRegistry();
    private Configuration defaultHadoopConfig;
    private String defaultSessionName = "default";

    public synchronized void init() {
        if (defaultHadoopConfig == null) {
            defaultHadoopConfig = loadSystemConfiguration();
        }

        // Create default session with system config and credentials
        if (!sessions.containsKey(defaultSessionName)) {
            try {
                CliSession defaultSession = createSession(defaultSessionName, defaultHadoopConfig, new DefaultCredentials());
                sessions.put(defaultSessionName, defaultSession);
            } catch (IOException e) {
                log.error("Failed to create default session", e);
            }
        }
    }

    public CliSession createSession(String name, Configuration config) throws IOException {
        return createSession(name, config, new DefaultCredentials());
    }

    public CliSession createSession(String name, Configuration config, SessionCredentials credentials) throws IOException {
        CliSession session = CliSession.builder()
            .withConfiguration(config)
            .withCredentials(credentials)
            .withCommandRegistry(defaultRegistry)
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

    public void removeSession(String name) {
        if (!name.equals(defaultSessionName)) {
            sessions.remove(name);
        }
    }

    public Set<String> listSessions() {
        return sessions.keySet();
    }

    public void addCommand(Command cmd) {
        defaultRegistry.register(cmd);
    }

    // Delegate to default session for backwards compatibility in CLI app
    public CommandReturn processInput(String line) throws DisabledException {
        return getDefaultSession().processInput(line);
    }

    private Configuration loadSystemConfiguration() {
        // Same logic as current init() for loading from HADOOP_CONF_DIR
    }
}
```

## Usage Examples

### Library Usage - Multiple Clusters

```java
// Build configurations for different clusters
Configuration prodConfig = new Configuration();
prodConfig.addResource(new Path("/etc/hadoop/conf-prod/core-site.xml"));
prodConfig.addResource(new Path("/etc/hadoop/conf-prod/hdfs-site.xml"));

Configuration devConfig = new Configuration();
devConfig.addResource(new Path("/etc/hadoop/conf-dev/core-site.xml"));
devConfig.addResource(new Path("/etc/hadoop/conf-dev/hdfs-site.xml"));

// Create command registry and register commands
CommandRegistry registry = new CommandRegistry();
registry.register(new Ls("ls"));
registry.register(new Cd("cd"));
// ... register other commands

// Create sessions with different credentials
CliSession prodSession = CliSession.builder()
    .withConfiguration(prodConfig)
    .withCredentials(new KeytabCredentials("svc-prod@REALM", "/etc/keytabs/prod.keytab"))
    .withCommandRegistry(registry)
    .build();

CliSession devSession = CliSession.builder()
    .withConfiguration(devConfig)
    .withCredentials(new KeytabCredentials("svc-dev@REALM", "/etc/keytabs/dev.keytab"))
    .withCommandRegistry(registry)
    .build();

// Execute commands on different clusters
CommandReturn prodResult = prodSession.processInput("ls /data/warehouse");
CommandReturn devResult = devSession.processInput("ls /data/warehouse");
```

### CLI App - Interactive Switching

```bash
# Start with default session (system config)
hadoop-cli> ls /user

# Create and switch to prod cluster
hadoop-cli> connect -n prod -c /etc/hadoop/conf-prod
Connected to 'prod'

hadoop-cli[prod]> ls /data

# Switch back to default
hadoop-cli[prod]> connect default
Connected to 'default'

hadoop-cli> sessions
  default (active)
  prod
```

## Migration Plan

### Files Requiring Changes

**New Files:**
- `src/main/java/com/cloudera/utils/hadoop/cli/session/SessionCredentials.java`
- `src/main/java/com/cloudera/utils/hadoop/cli/session/DefaultCredentials.java`
- `src/main/java/com/cloudera/utils/hadoop/cli/session/KeytabCredentials.java`
- `src/main/java/com/cloudera/utils/hadoop/cli/session/ProxyCredentials.java`
- `src/main/java/com/cloudera/utils/hadoop/cli/session/CliSessionBuilder.java`
- `src/main/java/com/cloudera/utils/hadoop/cli/CommandRegistry.java`

**Modified Files:**
- `CliSession.java` - Major expansion with processCommand, builder, credentials
- `CliEnvironment.java` - Become factory/registry, delegate to sessions
- `Command.java` - Change execute/implementation signature
- `AbstractCommand.java` - Update for CliSession
- `HdfsAbstract.java` - Remove env field, update PathBuilder usage
- `PathBuilder.java` - Accept CliSession instead of CliEnvironment
- All command implementations (~20+ files) - Update to use CliSession

**Command Files to Update:**
- `LocalCat.java`, `LocalCd.java`, `LocalHead.java`, `LocalLs.java`, `LocalMkdir.java`, `LocalPwd.java`, `LocalRm.java`
- `List.java`, `Use.java`, `SnapshotDiff.java`
- `HdfsSource.java`, `HdfsLsPlus.java`
- All other command implementations

**Spring Configuration:**
- `HadoopCliAppCfg.java` - Adjust command registration to use CommandRegistry

### Migration Steps

1. Create credentials abstraction classes
2. Create CommandRegistry class
3. Update CliSession with builder, processCommand, credentials support
4. Update Command interface and AbstractCommand
5. Update CliEnvironment to be factory/registry
6. Update HdfsAbstract and PathBuilder
7. Update all command implementations
8. Update Spring configuration
9. Add new CLI commands (connect, disconnect, sessions) - optional
10. Update tests

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Breaking change for external library users | Documented as clean break; provide migration guide |
| Thread safety with concurrent sessions | Use ConcurrentHashMap for session registry; each session is independent |
| Memory leaks from unclosed sessions | Document need to close/cleanup; consider AutoCloseable |
| Credential refresh complexity | SessionCredentials.refresh() method; caller responsibility |

## Future Considerations

- Session persistence (save/restore session configurations)
- Session timeouts and automatic cleanup
- Connection pooling for FileSystem instances
- Async command execution across multiple sessions
