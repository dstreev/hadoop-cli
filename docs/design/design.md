# HCFS Browser v5.0.4.0 -- Design Document

## 1. Project Overview

HCFS Browser is an interactive command-line interface (CLI) for navigating and managing Hadoop Compatible File Systems (HCFS). Built as a Spring Boot application targeting Java 11+, it provides a shell-like experience for interacting with HDFS, Ozone, and local file systems, along with YARN cluster inspection capabilities.

The tool serves as a unified interface for Hadoop filesystem operations, offering features beyond the standard `hdfs dfs` commands: multi-namespace navigation, Kerberos session management with UGI isolation, command pipelining, variable substitution, script execution, and YARN/NameNode statistics collection via REST and JMX endpoints.

### Key Capabilities

- Interactive shell with tab completion, command history, and ANSI-styled output
- Multi-namespace filesystem browsing (HDFS nameservices, Ozone service IDs, local filesystem)
- Kerberos authentication with keytab, current-user, and proxy-user credential strategies
- Multi-session architecture with isolated UserGroupInformation (UGI) per session
- Extended `lsp` command for detailed file listing with block-level inspection
- YARN ResourceManager and NameNode JMX statistics collection
- Batch script execution with template-based parameterization
- API mode for programmatic integration without interactive shell initialization

## 2. Design Goals and Principles

**Filesystem Abstraction.** Operate uniformly across HDFS nameservices, Ozone volumes, and local filesystems through the Hadoop `FileSystem` API, without filesystem-specific command variants.

**Session Isolation.** Each CLI session maintains its own Hadoop `Configuration`, `UserGroupInformation`, `CliFsShell`, and `FileSystemOrganizer`. Multiple sessions can authenticate as different Kerberos principals without interfering with each other.

**Minimal Global State.** Kerberos global state (the single required call to `UserGroupInformation.setConfiguration()`) is initialized exactly once through `KerberosGlobalState`. All subsequent session-level authentication uses isolated UGI instances.

**Delegate to Hadoop.** Filesystem commands delegate to the Hadoop `FsShell` command infrastructure via `CliFsShell`, which extends `FsShell`. This ensures compatibility with the full HDFS command set while allowing output capture and redirection.

**Spring Boot Integration.** Application lifecycle, configuration binding, and component wiring are managed by Spring Boot. Command-line arguments are translated to Spring Boot properties, enabling conditional feature activation through `@ConditionalOnProperty` annotations.

**Extensibility.** New commands are added by implementing the `Command` interface and registering them in `CommandRegistry`. The command framework supports custom options via Apache Commons CLI and tab completion via JLine.

## 3. Component Design

### 3.1 Component Overview Diagram

```
+------------------------------------------------------------------+
|                        HCFSCliApp (main)                         |
|                    @SpringBootApplication                        |
+------------------------------------------------------------------+
         |                        |                       |
         v                        v                       v
+------------------+   +--------------------+   +------------------+
| HadoopCliCommand |   | HadoopCliAppCfg    |   | Shell            |
| LineOptions      |   | @Configuration     |   | @Component       |
| (arg translation)|   | (CommandLineRunner |   | (interactive     |
|                  |   |  beans, lifecycle) |   |  REPL loop)      |
+------------------+   +--------------------+   +------------------+
                                |                       |
                                v                       v
                      +--------------------+   +------------------+
                      | CliEnvironment     |   | ConsoleReader    |
                      | @Component         |   | (JLine)          |
                      | (session mgmt,     |   | - tab completion |
                      |  command dispatch) |   | - history        |
                      +--------------------+   +------------------+
                                |
                +---------------+----------------+
                |                                |
                v                                v
       +------------------+            +------------------+
       | CliSession       |            | CommandRegistry   |
       | (per-session     |            | @Component        |
       |  state, UGI,     |            | (command lookup,  |
       |  command exec)   |            |  registration)    |
       +------------------+            +------------------+
           |           |                        |
           v           v                        v
   +------------+  +------------------+  +------------------+
   | CliFsShell |  | FileSystem       |  | Command impls    |
   | (extends   |  | Organizer        |  | (HdfsCommand,    |
   |  FsShell)  |  | (namespace mgmt) |  |  HdfsLsPlus,    |
   +------------+  +------------------+  |  ContainerStats, |
                                         |  SchedulerStats) |
                                         +------------------+
```

### 3.2 Component Responsibilities

**HCFSCliApp** (`com.cloudera.utils.hcfs.HCFSCliApp`): Application entry point. Translates raw command-line arguments to Spring Boot properties via `HadoopCliCommandLineOptions`, then delegates to `SpringApplication.run()`.

**HadoopCliCommandLineOptions** (`com.cloudera.utils.hcfs.cli.HadoopCliCommandLineOptions`): Parses the original CLI arguments (`-i`, `-e`, `-f`, `-s`, `-v`, `-d`, `--api`, `--env-file`, etc.) and converts them to Spring Boot `--hadoop.cli.*` properties. This bridge allows the application to use Spring Boot's property-driven configuration while accepting traditional CLI syntax.

**HadoopCliAppCfg** (`com.cloudera.utils.hcfs.cli.HadoopCliAppCfg`): Spring `@Configuration` class that defines ordered `CommandLineRunner` beans. Each runner is conditionally activated based on properties (`hadoop.cli.init`, `hadoop.cli.execute`, `hadoop.cli.verbose`, etc.). The interactive shell runner (`runInteractiveShell`) is gated by `hadoop-cli.api.enabled=false` via a class-level `@ConditionalOnProperty`, ensuring the shell does not start in API mode.

**Shell** (`com.cloudera.utils.hcfs.cli.Shell`): The interactive REPL. Initializes JLine `ConsoleReader` with tab completers built from the `CommandRegistry`, sets up file-based command history at `~/.hadoop-cli/history`, displays the startup banner, creates the `cli` session, and enters the read-eval-print loop. Each iteration reads a line, calls `CliSession.processInput()`, and renders the result with ANSI coloring.

**CliEnvironment** (`com.cloudera.utils.hcfs.cli.CliEnvironment`): Spring-managed singleton that owns the session map (`ConcurrentHashMap<String, CliSession>`), the shared `CommandRegistry`, environment properties, and Hadoop configuration loading logic. Provides session lifecycle methods (`createSession`, `getOrCreateSession`, `removeSession`, `listSessions`) and backward-compatible delegation methods that route to the current session.

**CliSession** (`com.cloudera.utils.hcfs.cli.CliSession`): Per-session state container. Each session holds its own `Configuration`, `SessionCredentials`, `UserGroupInformation`, `CliFsShell`, `FileSystemOrganizer`, and `CommandRegistry` reference. Constructed via a builder pattern. On `init()`, determines the authentication mode (Kerberos vs. simple), obtains or creates a UGI, and initializes the shell and filesystem within the appropriate security context using `UGI.doAs()`.

**CommandRegistry** (`com.cloudera.utils.hcfs.cli.session.CommandRegistry`): Maintains a `TreeMap<String, Command>` of all available commands. Populated in its constructor with the full command set: HDFS operations, local filesystem operations, snapshot management, YARN statistics, and utility commands. Shared across sessions since commands themselves are stateless -- session-specific state is passed via the `CliSession` parameter in `Command.execute()`.

**CliFsShell** (`org.apache.hadoop.fs.CliFsShell`): Extends Hadoop's `FsShell` to allow `PrintStream` redirection for stdout and stderr. Uses `CliCommandFactory` (which extends Hadoop's `CommandFactory`) to create command instances with redirected output streams. The `init()` method no longer calls `UserGroupInformation.setConfiguration()` to avoid polluting global state; Kerberos initialization is handled once by `KerberosGlobalState`.

**FileSystemOrganizer** (`com.cloudera.utils.hcfs.hdfs.util.FileSystemOrganizer`): Manages multiple `FileSystemState` entries representing available namespaces. During `init()`, discovers HDFS nameservices from `dfs.nameservices`, Ozone services from `ozone.om.service.ids` / `ozone.service.id`, and creates a local filesystem state. Tracks the current working namespace and provides the shell prompt showing the current working directory and URI.

## 4. Authentication Design

### 4.1 Credential Strategy Pattern

Authentication is modeled through the `SessionCredentials` interface with three implementations:

```
            +---------------------+
            | <<interface>>       |
            | SessionCredentials  |
            +---------------------+
            | + getUGI(): UGI     |
            | + refresh(): void   |
            +---------------------+
                 ^    ^    ^
                 |    |    |
    +------------+    |    +-------------+
    |                 |                  |
+---+--------+ +-----+------+ +--------+--------+
| Default    | | Keytab     | | Proxy            |
| Credentials| | Credentials| | Credentials      |
+------------+ +------------+ +------------------+
| getCurrent | | loginFrom  | | createProxyUser  |
| User()     | | Keytab()   | | (wraps real      |
+------------+ +------------+ |  credentials)    |
                               +------------------+
```

**DefaultCredentials**: Returns `UserGroupInformation.getCurrentUser()`. Used when the user has already authenticated externally (e.g., via `kinit`) or in simple authentication mode.

**KeytabCredentials**: Accepts a Kerberos principal and keytab file path. Lazily calls `UserGroupInformation.loginUserFromKeytabAndReturnUGI()` on first `getUGI()` invocation. This method creates an isolated UGI without modifying the JVM's global login state. Thread-safe via `synchronized`. Supports credential refresh via `checkTGTAndReloginFromKeytab()`.

**ProxyCredentials**: Wraps another `SessionCredentials` instance (the "real" credentials) and creates a proxy UGI via `UserGroupInformation.createProxyUser()`. On `refresh()`, refreshes the underlying real credentials and invalidates the cached proxy UGI so it gets recreated with fresh credentials on the next access.

### 4.2 KerberosGlobalState

The JVM-global Kerberos initialization is managed by `KerberosGlobalState`, a utility class using double-checked locking with a `volatile` flag. The first call to `ensureInitialized()` creates a minimal `Configuration` with `hadoop.security.authentication=kerberos` and calls `UserGroupInformation.setConfiguration()`. Subsequent calls are no-ops. This design is necessary because `loginUserFromKeytabAndReturnUGI()` checks `UserGroupInformation.isSecurityEnabled()`, which depends on the global configuration.

### 4.3 Authentication Flow

```
CliSession.init()
  |
  +-- Is Kerberos enabled? (hadoop.security.authentication == "kerberos")
  |     |
  |     +-- YES:
  |     |     +-- KerberosGlobalState.ensureInitialized()  [one-time global init]
  |     |     +-- credentials != null?
  |     |     |     +-- YES: ugi = credentials.getUGI()   [isolated UGI]
  |     |     |     +-- NO:  ugi = getLoginUser()          [fallback]
  |     |     +-- ugi.doAs(() -> initializeShellAndFileSystem())
  |     |
  |     +-- NO (simple auth):
  |           +-- ugi = null
  |           +-- initializeShellAndFileSystem() [direct call]
```

### 4.4 Command Execution with UGI

When a command is executed in a Kerberos-enabled session, `CliSession.processCommand()` wraps the execution in `ugi.doAs()`. If authentication fails with "Client cannot authenticate via:[TOKEN, KERBEROS]", the session retries up to 7 times, calling `credentials.refresh()` (or `ugi.checkTGTAndReloginFromKeytab()` for fallback) and waiting 2 seconds between attempts. This handles the initial warm-up of Kerberos tickets.

## 5. Multi-Session Architecture

### 5.1 Session Lifecycle

```
CliEnvironment
  |
  +-- sessions: ConcurrentHashMap<String, CliSession>
  |     |
  |     +-- "cli"     -> CliSession [created by Shell.startShell()]
  |     +-- "default" -> CliSession [if created programmatically]
  |     +-- "custom"  -> CliSession [user-created sessions]
  |
  +-- currentSessionName: String (tracks active session)
  +-- defaultSessionName: String ("default")
```

Sessions are created via `CliEnvironment.createSession(name, config, credentials)`. If a session with the given name already exists, the existing session is returned (idempotent creation). Sessions are stored in a `ConcurrentHashMap` for thread-safe access.

Each session is fully independent:
- **Configuration**: Each session can have a different `hadoop.security.authentication`, `fs.defaultFS`, etc.
- **UGI**: Each session has its own `UserGroupInformation`, potentially authenticated as a different principal.
- **FileSystemOrganizer**: Each session discovers and tracks its own set of namespaces.
- **CliFsShell**: Each session has its own `FsShell` instance with redirectable output streams.

The default session cannot be removed (`removeSession` silently ignores the default name).

### 5.2 UGI Isolation Diagram

```
JVM Process
+------------------------------------------------------------------+
| KerberosGlobalState (initialized ONCE)                           |
|   UserGroupInformation.setConfiguration(kerberos-config)         |
+------------------------------------------------------------------+
         |
         +-- Session "admin"
         |     UGI: admin@REALM (KeytabCredentials)
         |     Shell -> FileSystem (as admin)
         |     All commands run via admin.doAs(...)
         |
         +-- Session "analyst"
         |     UGI: analyst@REALM (KeytabCredentials)
         |     Shell -> FileSystem (as analyst)
         |     All commands run via analyst.doAs(...)
         |
         +-- Session "proxy-user"
               UGI: proxyUser (ProxyCredentials wrapping admin keytab)
               Shell -> FileSystem (as proxyUser via admin)
               All commands run via proxyUGI.doAs(...)
```

## 6. Command Processing Pipeline

### 6.1 Input Processing Flow

```
User Input (raw string)
  |
  v
CliSession.processInput(line)
  |
  +-- substituteVariables(line)     -- replace ${var} and $var references
  |                                    with system properties or session properties
  v
  +-- Split on pipe '|'             -- support single-depth pipelining
  |     (max 2 commands)
  v
  +-- For each pipeline segment:
        |
        +-- processCommand(segment, previousCR)
              |
              +-- If UGI != null: ugi.doAs(() -> processCommandInternal(...))
              |     with retry logic for Kerberos warm-up
              +-- Else: processCommandInternal(...)
                    |
                    +-- parseArguments(line)   -- regex tokenizer, handles
                    |                             quoted strings
                    +-- Lookup command in CommandRegistry
                    +-- Parse options via Commons CLI PosixParser
                    +-- command.execute(session, commandLine, commandReturn)
                          |
                          +-- AbstractCommand.execute()
                                +-- implementation(session, cmd, cr)
                                      |
                                      +-- HdfsCommand: delegates to CliFsShell.run()
                                      +-- HdfsLsPlus: uses DFSClient for block details
                                      +-- ContainerStats: queries YARN REST API
                                      +-- Env/Help/Exit: utility operations
```

### 6.2 Command Argument Parsing

Arguments are tokenized using a regex pattern that preserves quoted strings:

```
Pattern: [^\s"']+|"([^"]*)"|'([^']*)'
```

This allows commands like: `ls "path with spaces"` or `chown 'user:group' /path`.

### 6.3 Pipeline Support

The system supports single-depth command pipelining using the `|` character. The output of the first command (accessed via `CommandReturn.getReturn()`) is read line-by-line, and each line is appended as an argument to the second command. Lines containing spaces are automatically quoted.

### 6.4 Variable Substitution

Two variable reference patterns are supported:
- `${variable.name}` -- standard form with braces
- `$variable` -- short form (terminated by whitespace or `/`)

Variables are resolved in order: Java system properties first, then session properties (set via `--env-file` or programmatically).

## 7. Command Categories

### 7.1 HDFS File Operations

Commands that delegate to `CliFsShell` (Hadoop's `FsShell`):

| Command | Direction | Description |
|---------|-----------|-------------|
| `ls`, `lsr` | NONE | List directory contents |
| `cd` | -- | Change working directory |
| `pwd` | -- | Print working directory |
| `mkdir` | NONE | Create directories |
| `rm`, `rmdir` | NONE | Remove files/directories |
| `cat`, `text` | NONE | Display file contents |
| `head`, `tail` | NONE | Display file head/tail |
| `put`, `copyFromLocal` | LOCAL_REMOTE | Upload files |
| `get`, `copyToLocal` | REMOTE_LOCAL | Download files |
| `cp` | REMOTE_REMOTE | Copy within HDFS |
| `mv` | REMOTE_REMOTE | Move/rename |
| `chown`, `chmod`, `chgrp` | NONE | Change ownership/permissions |
| `du`, `df`, `dus` | NONE | Disk usage |
| `count`, `stat` | NONE | File statistics |
| `touchz` | NONE | Create empty file |
| `checksum` | NONE | File checksum |
| `test` | NONE | Test file existence |

The `Direction` enum controls how path arguments are resolved relative to the current filesystem context.

### 7.2 Extended HDFS Commands

| Command | Description |
|---------|-------------|
| `lsp` | Enhanced listing with block-level details (replication, block size, datanode locations, storage policy, erasure coding info) via DFSClient |
| `connect` | Connect to an HDFS endpoint |
| `use` | Switch active namespace |
| `list` / `nss` / `namespaces` | List available namespaces |

### 7.3 Snapshot Commands

| Command | Description |
|---------|-------------|
| `allowSnapshot` | Enable snapshots on a directory |
| `disallowSnapshot` | Disable snapshots on a directory |
| `lsSnapshottableDir` | List snapshottable directories |
| `createSnapshot` | Create a snapshot |
| `deleteSnapshot` | Delete a snapshot |
| `renameSnapshot` | Rename a snapshot |
| `snapshotDiff` | Show differences between snapshots |

### 7.4 Local File Operations

| Command | Description |
|---------|-------------|
| `lls` | Local ls |
| `lpwd` | Local pwd |
| `lcd` | Local cd |
| `lhead` | Local head |
| `lcat` | Local cat |
| `lmkdir` | Local mkdir |
| `lrm` | Local rm |

### 7.5 YARN Statistics Commands

| Command | Description |
|---------|-------------|
| `cstat` | Container statistics from ResourceManager REST API |
| `sstat` | Scheduler statistics from ResourceManager REST API |

### 7.6 Utility Commands

| Command | Description |
|---------|-------------|
| `env` | Display/modify environment properties |
| `help` | Show available commands and usage |
| `history` | Show command history |
| `source` | Execute commands from a file |
| `exit` | Exit the shell |

## 8. Network Communication Design

### 8.1 HDFS Communication

All HDFS filesystem operations go through the Hadoop `FileSystem` API, which communicates with the NameNode via the HDFS RPC protocol. The `FileSystem` instance is created via `FileSystem.newInstance(config)` during `FileSystemOrganizer.init()`.

For extended operations (`lsp` command), the `HdfsLsPlus` command uses the `DFSClient` directly to obtain block-level information (`LocatedBlocks`, `DatanodeInfo`) that is not available through the standard `FileSystem` API.

### 8.2 NameNode JMX Integration

`HdfsNNStats` collects NameNode metrics by querying JMX HTTP endpoints:

```
http://<namenode-host>:<http-port>/jmx?qry=<bean-name>
```

Target JMX beans:
- `Hadoop:service=NameNode,name=NameNodeInfo` -- version, usage, safemode, block counts
- `Hadoop:service=NameNode,name=FSNamesystem` -- filesystem state, datanode counts, replication metrics
- `Hadoop:service=NameNode,name=NameNodeStatus` -- active/standby state

In HA configurations, the tool resolves NameNode addresses from `dfs.ha.namenodes.<nameservice>` and `dfs.namenode.http-address.<nameservice>.<nn-id>` configuration properties.

### 8.3 YARN ResourceManager REST API

`ResourceManagerStats` (base class for `ContainerStats` and `SchedulerStats`) communicates with YARN via REST:

- **RM Discovery**: In HA mode, iterates through `yarn.resourcemanager.ha.rm-ids`, queries each RM's `/ws/v1/cluster/info` endpoint to find the ACTIVE instance.
- **Protocol Selection**: Supports HTTP and HTTPS based on the `--ssl` flag, using `yarn.resourcemanager.webapp.http.address.<rm-id>` or `yarn.resourcemanager.webapp.https.address.<rm-id>` accordingly.
- **Time-Windowed Queries**: Supports `--last` (relative time window), `--start`/`--end` (absolute time range), and `--increment` (query batching interval) options to partition large time ranges into manageable queries.

```
ResourceManager REST API
  |
  +-- /ws/v1/cluster/info          (RM state discovery)
  +-- /ws/v1/cluster/apps          (application listing, time-windowed)
  +-- /ws/v1/cluster/scheduler     (scheduler queue information)
```

## 9. Configuration Management

### 9.1 Hadoop Configuration Loading

Configuration is loaded in `CliEnvironment.loadDefaultConfiguration()` with the following priority:

1. `HADOOP_CONF_DIR` environment variable (if set and directory exists)
2. `/etc/hadoop/conf` (fallback default path)
3. Hadoop built-in defaults (if no configuration directory is found)

Configuration files loaded from the directory:
- `core-site.xml`
- `hdfs-site.xml`
- `mapred-site.xml`
- `yarn-site.xml`
- `ozone-site.xml`

After loading XML files, any properties set on `CliEnvironment` are applied as overrides.

### 9.2 Spring Boot Configuration

The `application.yaml` configures:

```yaml
spring:
  main:
    banner-mode: "off"           # Suppress Spring Boot banner (custom banner used)
    web-application-type: NONE   # No web server -- CLI-only application
hadoop-cli:
  api:
    enabled: false               # Default: interactive shell mode
```

### 9.3 Command-Line to Spring Property Mapping

`HadoopCliCommandLineOptions.toSpringBootOption()` translates CLI arguments:

| CLI Argument | Spring Property |
|---|---|
| `-i <file>` | `--hadoop.cli.init=<file>` |
| `-e <cmd>` | `--hadoop.cli.execute=<cmd>` |
| `-f <file>` | `--hadoop.cli.file=<file>` |
| `-t <tmpl>` | `--hadoop.cli.template=<tmpl>` |
| `-td <delim>` | `--hadoop.cli.template-delimiter=<delim>` |
| `-s` | `--hadoop.cli.silent=true` |
| `-v` | `--hadoop.cli.verbose=true` |
| `-d` | `--hadoop.cli.debug=true` |
| `--api` | `--hadoop.cli.api=true` |
| `--env-file <file>` | `--hadoop.cli.env-file=<file>` |

## 10. CLI Features

### 10.1 Tab Completion

Tab completion is powered by JLine 2. The `Shell` component builds an `AggregateCompleter` from individual `ArgumentCompleter` instances -- one per registered command. Each command can provide its own `Completer` implementation; for example, `HdfsConnect` offers a `StringsCompleter` with common HDFS URIs. The `FileSystemNameCompleter` and `NamespaceCompleter` provide context-aware completion for filesystem paths and namespace names.

### 10.2 Command History

Command history is persisted to `~/.hadoop-cli/history` using JLine's `FileHistory`. The history is flushed on JVM shutdown via a shutdown hook. The `history` command allows users to view past commands within the session.

### 10.3 Script Execution

The `source` command and `-f` / `-i` CLI options execute commands from a file. Each line is processed sequentially through `processInput()`. Lines starting with `#` are treated as comments. The `-t` (template) option allows parameterized execution: each input line is split by the delimiter (default `,`), and values are substituted into a `MessageFormat` template.

Example with template:
```
# Input file (data.csv):
/user/alice,755
/user/bob,700

# Template: chmod {1} {0}
# Produces: chmod 755 /user/alice
#           chmod 700 /user/bob
```

### 10.4 Output Formatting

Command output supports ANSI styling through the `ANSIStyle` utility class and `CommandReturn.getStyledReturn()`. The prompt displays the current working directory in magenta and the filesystem URI in yellow. Success output is rendered in green; errors display error codes, commands, and messages in red.

## 11. API Mode

When launched with `--api` (or `hadoop-cli.api.enabled=true`), the application skips interactive shell initialization. The `HadoopCliAppCfg` class is entirely disabled via the class-level `@ConditionalOnProperty(name = "hadoop-cli.api.enabled", havingValue = "false")` annotation, preventing the `runInteractiveShell` bean from being created.

In API mode, consuming applications can:
1. Obtain a `CliEnvironment` instance from the Spring context
2. Create sessions with custom configurations and credentials via `createSession()`
3. Execute commands programmatically via `CliSession.processInput()` or `CliEnvironment.processInput()`
4. Inspect results through the `CommandReturn` object (return code, output, error, styled output)

The API mode also prevents the JLine `ConsoleReader` from being initialized, which avoids issues when the application is embedded in environments without a terminal (e.g., application servers, batch processing pipelines).

## 12. Key Dependencies and Their Roles

| Dependency | Version | Role |
|---|---|---|
| **Spring Boot** | 3.4.1 | Application framework, dependency injection, configuration binding, lifecycle management |
| **Apache Hadoop (hadoop-client-api, hadoop-client-runtime)** | 3.4.1 | HDFS/YARN client libraries, FileSystem API, UserGroupInformation, FsShell infrastructure |
| **JLine 2** | 2.14.6 | Interactive console: line editing, tab completion, history |
| **Jansi** | 2.4.1 | ANSI escape code support for terminal colors and formatting |
| **Apache Commons CLI** | 1.4 | Command-line option parsing for both the application and individual commands |
| **Apache Commons IO** | 2.18.0 | I/O utilities (URL content reading for REST/JMX endpoints) |
| **Apache Commons Lang3** | 3.17.0 | String and number utilities |
| **Jackson** | (via Spring Boot) | JSON parsing for YARN REST API and NameNode JMX responses |
| **Lombok** | (via Spring Boot) | Boilerplate reduction: `@Getter`, `@Setter`, `@Slf4j`, `@Builder` |
| **JCabi Manifests** | 2.1.0 | JAR manifest attribute reading for version display in banner |
| **SLF4J + Logback** | (via Spring Boot) | Logging framework with custom `logback-hcfs-cli.xml` configuration |
| **Jackson Databind** | 2.18.2 | JSON serialization/deserialization for REST API responses |
| **Gateway Shell** | 2.1.0 | Knox Gateway integration (dependency present but usage limited) |

## 13. Namespace Management

### 13.1 FileSystemOrganizer State Model

```
FileSystemOrganizer
  |
  +-- namespaces: TreeMap<String, FileSystemState>
  |     |
  |     +-- "NAMESERVICE1" -> FileSystemState {hdfs://, ns1, workingDir=/}
  |     +-- "NAMESERVICE2" -> FileSystemState {hdfs://, ns2, workingDir=/}
  |     +-- "OZONE-SVC"    -> FileSystemState {ofs://, ozone-svc, workingDir=/}
  |     +-- "LOCAL"         -> FileSystemState {file:, local, workingDir=cwd}
  |
  +-- defaultFileSystemState  -> points to ns matching fs.defaultFS
  +-- currentFileSystemState  -> points to active namespace
```

Namespace discovery during `init()`:
1. Read `dfs.nameservices` -- split by comma, create `FileSystemState` for each
2. Match against `fs.defaultFS` to identify the default namespace
3. Read `ozone.om.service.ids` -- create `FileSystemState` entries with `ofs://` protocol
4. Read `ozone.service.id` -- create default Ozone state
5. Create `LOCAL` entry backed by the local filesystem

The `use` command switches the current namespace. The `list`/`nss`/`namespaces` command displays all discovered namespaces.

## 14. Error Handling

### 14.1 Error Code System

Commands return results via `CommandReturn`, which carries:
- `code`: Integer status (`GOOD = 0`, various negative codes for errors)
- `return`: String output (success result)
- `error`: String error message
- `command`: The command string that was executed
- Output streams (`PrintStream out/err`) for streaming output

Standard error codes defined in `AbstractCommand`:

| Code | Constant | Meaning |
|---|---|---|
| 0 | `CODE_SUCCESS` | Command succeeded |
| -1 | `CODE_CMD_ERROR` | General command error |
| -10 | `CODE_NOT_CONNECTED` | Not connected to filesystem |
| -11 | `CODE_CONNECTION_ISSUE` | Connection failure |
| -20 | `CODE_PATH_ERROR` | Invalid path |
| -100 | `CODE_FS_CLOSE_ISSUE` | Filesystem close error |
| -123 | `CODE_LOCAL_FS_ISSUE` | Local filesystem error |
| -200 | `CODE_STATS_ISSUE` | Statistics collection error |
| -321 | `CODE_BAD_DATE` | Invalid date format |
| 1 | `CODE_NOT_FOUND` | Resource not found |

### 14.2 Kerberos Retry Logic

Authentication failures trigger a retry loop in `CliSession.processCommand()`:
- Maximum 7 attempts
- 2-second delay between attempts
- Credential refresh before each retry
- `DisabledException` thrown if all attempts fail

## 15. Build and Packaging

The project is built with Maven and produces a shaded JAR (`maven-shade-plugin`) that bundles all dependencies. Key build configuration:

- **Source/Target**: Java 11
- **JDK Toolchain**: Java 17 (build environment)
- **Shading**: Relocates conflicting packages; excludes signature files (`META-INF/*.SF`, `META-INF/*.DSA`, `META-INF/*.RSA`)
- **Main Class**: `com.cloudera.utils.hcfs.HCFSCliApp` (specified in the shaded JAR manifest)
- **Artifact**: `hcfs-browser-5.0.4.0.jar`

The `maven-assembly-plugin` creates a distribution archive including the shaded JAR, launch scripts, and configuration templates.
