# HCFS Browser - Architecture Document

**Project:** HCFS Browser (Hadoop Compatible File System Browser Interface)
**Version:** 5.0.4.0
**Group:** com.cloudera.utils.hadoop
**License:** Apache License 2.0

---

## 1. System Context and Purpose

HCFS Browser is an interactive command-line interface (CLI) for browsing and managing Hadoop Compatible File Systems. It provides a shell experience with tab completion, command history, and dual working directories (local and remote HDFS) within a single session. The application also serves as an embeddable API for programmatic access to HDFS, YARN, and MapReduce cluster resources.

The system operates as a non-web Spring Boot application that wraps the Hadoop FileSystem API, YARN Resource Manager REST API, and NameNode JMX endpoints into a unified, interactive command environment.

### Primary Capabilities

- Interactive HDFS file browsing with working directory tracking per namespace
- Native Hadoop `hdfs dfs` command delegation via `CliFsShell`/`ToolRunner`
- Multi-namespace support: HDFS nameservices, Ozone object storage (ofs://), and local filesystem
- YARN container and scheduler statistics collection via REST API
- NameNode JMX metric extraction and reporting
- Kerberos authentication with per-session UGI isolation
- Multi-session support with independent credential and filesystem contexts
- Embeddable API mode with shell initialization suppression

---

## 2. Component Architecture

### 2.1 Package Overview

```
com.cloudera.utils.hcfs
|
+-- HCFSCliApp                          # Spring Boot entry point
|
+-- cli/                                # CLI framework and session management
|   +-- Shell                           # Interactive REPL (JLine console)
|   +-- CliEnvironment                  # Spring component, session factory, config loader
|   +-- CliSession                      # Per-session state: config, UGI, commands, filesystem
|   +-- HadoopCliAppCfg                 # Spring @Configuration, CommandLineRunner beans
|   +-- HadoopCliCommandLineOptions     # CLI arg parsing, Spring Boot option translation
|   +-- DisabledException               # Thrown when CLI is disabled
|   +-- session/
|       +-- SessionCredentials          # Interface: getUGI(), refresh()
|       +-- DefaultCredentials          # Current user via kinit
|       +-- KeytabCredentials           # Keytab-based login, isolated UGI
|       +-- ProxyCredentials            # Proxy user delegation
|       +-- KerberosGlobalState         # Singleton: one-time UGI.setConfiguration()
|       +-- CommandRegistry             # TreeMap<String,Command>, command lookup
|
+-- shell/                             # Abstract command framework
|   +-- command/
|   |   +-- Command                     # Interface: execute(), getOptions(), getCompleter()
|   |   +-- AbstractCommand             # Base impl: error codes, verbose/debug logging
|   |   +-- CommandReturn               # Result container: code, stdout, stderr, ANSI styles
|   +-- commands/
|   |   +-- Help, Exit, Env, HistoryCmd # Built-in shell commands
|   +-- format/
|       +-- ANSIStyle                   # ANSI escape code formatting utilities
|
+-- hdfs/
|   +-- shell/command/                  # HDFS command implementations
|   |   +-- HdfsAbstract               # Base for HDFS commands (PathDirectives)
|   |   +-- HdfsCommand                # Delegates to CliFsShell via ToolRunner
|   |   +-- HdfsCd, HdfsPwd            # Working directory management
|   |   +-- HdfsConnect                # Connection/namespace switching
|   |   +-- HdfsAllowSnapshot, etc.    # Snapshot management commands
|   |   +-- SnapshotDiff               # Snapshot comparison
|   |   +-- LocalLs, LocalCd, etc.     # Local filesystem commands
|   |   +-- Use, List                   # Namespace selection and listing
|   |   +-- PathBuilder                # Context-aware path resolution
|   |   +-- PathDirectives, Direction  # Path operation semantics (REMOTE_LOCAL, etc.)
|   |   +-- Side, ContextSide          # Left/right path selection for copy operations
|   |   +-- FSUtil                     # Filesystem utility operations
|   |   +-- Constants                  # Shared constants (LOCAL_FS key, etc.)
|   +-- shell/completers/
|   |   +-- FileSystemNameCompleter    # Tab completion for HDFS paths
|   |   +-- NamespaceCompleter         # Tab completion for available namespaces
|   +-- util/
|   |   +-- FileSystemOrganizer       # Tracks all namespaces and current filesystem state
|   |   +-- FileSystemState            # Per-namespace state: protocol, working dir, FileSystem
|   |   +-- HdfsLsPlus                # Enhanced ls with extended attributes
|   |   +-- HdfsSource                # Source file execution command
|   |   +-- HdfsNNStats               # NameNode statistics collection
|   |   +-- HdfsRepair                # HDFS repair utilities
|   |   +-- NamenodeJmxParser         # NameNode JMX bean extraction
|   |   +-- NamenodeJmxBean           # JMX bean name constants (enum)
|   |   +-- JmxJsonParser             # Generic JMX JSON response parser
|   |   +-- ReplicationManager        # Replication management
|   +-- replication/
|       +-- Definition, Endpoint      # Replication topology model
|       +-- DfsNameService            # Nameservice abstraction
|       +-- ReplicationHelper         # Replication operations
|       +-- SnapshotNamePattern       # Snapshot naming conventions
|       +-- NamenodeHost, State       # NameNode state tracking
|       +-- SyncStats                 # Replication sync statistics
|
+-- yarn/                              # YARN cluster interaction
|   +-- ResourceManagerStats           # Abstract: RM REST API client, HA failover
|   +-- ContainerStats                 # Container metric collection from RM REST
|   +-- ContainerStatsCommand          # CLI command wrapper for container stats
|   +-- SchedulerStats                # Queue/scheduler metric collection
|   +-- SchedulerStatsCommand          # CLI command wrapper for scheduler stats
|   +-- Stats                          # Interface for stats operations
|   +-- YarnAppRecordConverter         # YARN application record transformation
|   +-- parsers/
|       +-- QueueParser                # YARN queue hierarchy JSON parser
|
+-- mapreduce/                          # MapReduce job analysis
|   +-- JhsStats                       # Job History Server statistics
|   +-- MRJobRecordConverter           # MapReduce job record transformation
|   +-- parsers/
|       +-- JobCounterGroupParser      # Job counter group parsing
|       +-- TaskCounterGroupParser     # Task counter group parsing
|
+-- util/                              # Shared utilities
|   +-- RecordConverter                # Generic record transformation
|   +-- TraversePath                   # Directory tree traversal
|   +-- TraverseBehavior               # Traversal strategy interface
|   +-- NodeParser                     # Generic node parsing utility
|   +-- HdfsWriter                    # HDFS file write operations
|
+-- AbstractStats                      # Base class for stats commands
+-- AbstractQueryTimeFrameStats        # Time-windowed query support

org.apache.hadoop.fs                    # Hadoop package extensions
+-- CliFsShell                         # Extended FsShell with redirectable I/O
+-- shell/
    +-- CliCommandFactory              # Extended CommandFactory with I/O redirect
```

### 2.2 Class Relationship Summary

```
HCFSCliApp (main)
    |
    v
SpringApplication.run()
    |
    v
HadoopCliAppCfg (@Configuration)
    |-- configInit, configExecute, etc.  (CommandLineRunner beans, @Order)
    |-- runInteractiveShell(@Order=100)
    |
    v
Shell (@Component)
    |-- CliEnvironment (@Component)
    |       |-- CommandRegistry (@Component)
    |       |-- Map<String, CliSession> sessions
    |       |-- loadDefaultConfiguration()
    |       |
    |       +-- createSession(name, config, credentials)
    |               |
    |               v
    |           CliSession.builder().build()
    |               |-- KerberosGlobalState.ensureInitialized()
    |               |-- credentials.getUGI()
    |               |-- ugi.doAs(() -> initializeShellAndFileSystem())
    |               |       |-- new CliFsShell(config).init()
    |               |       +-- new FileSystemOrganizer().init(config)
    |               |
    |               +-- processInput(line) -> processCommand() -> ugi.doAs()
    |
    +-- acceptCommands(reader, session)  [REPL loop]
```

---

## 3. Spring Boot Integration

### 3.1 Application Configuration

The application uses Spring Boot 3.4.13 configured as a **non-web application** (`spring.main.web-application-type=NONE`). This prevents the embedded web server from starting, keeping the application as a pure CLI tool.

**application.yaml:**
```yaml
spring:
  main:
    banner-mode: "off"
    web-application-type: NONE
hadoop-cli:
  api:
    enabled: false
```

### 3.2 Spring Component Model

| Component | Annotation | Role |
|-----------|-----------|------|
| `HCFSCliApp` | `@SpringBootApplication` | Entry point, component scan root |
| `HadoopCliAppCfg` | `@Configuration`, `@ConditionalOnProperty` | Bean definitions, disabled in API mode |
| `Shell` | `@Component` | Interactive console REPL |
| `CliEnvironment` | `@Component` | Session factory, config management |
| `CommandRegistry` | `@Component` | Command registration and lookup |
| `HadoopCliCommandLineOptions` | `@Component`, `@Order(1)` | CLI argument translation |

### 3.3 CommandLineRunner Ordering

Spring Boot `CommandLineRunner` beans execute in `@Order` sequence:

| Order | Bean | Condition | Purpose |
|-------|------|-----------|---------|
| 1 | `configSilent` | `hadoop.cli.silent` | Suppress banner output |
| 1 | `configVerbose` | `hadoop.cli.verbose` | Enable verbose mode |
| 1 | `configDebug` | `hadoop.cli.debug` | Enable debug mode |
| 1 | `configApiMode` | `hadoop.cli.api` | Enable API mode |
| 1 | `configDisabled` | `hadoop.cli.disabled` | Disable CLI |
| 1 | `configTemplate` | `hadoop.cli.template` | Set template for file processing |
| 1 | `configExecute` | `hadoop.cli.execute` | Execute a single command |
| 50 | `configEnvFile` | `hadoop.cli.env-file` | Load environment properties file |
| 100 | `configInit` | `hadoop.cli.init` | Run initialization script |
| 100 | `runInteractiveShell` | (always, unless API mode) | Launch interactive shell |

### 3.4 API Mode

When `hadoop-cli.api.enabled=true`, the `HadoopCliAppCfg` configuration class is excluded entirely via `@ConditionalOnProperty(name="hadoop-cli.api.enabled", havingValue="false", matchIfMissing=true)`. This prevents shell initialization and allows the library to be used programmatically through `CliEnvironment` and `CliSession` directly.

### 3.5 Command-Line Argument Translation

`HadoopCliCommandLineOptions.toSpringBootOption()` converts traditional CLI flags to Spring Boot property format:

```
Input:   -i init.txt -v -e "ls /"
Output:  --hadoop.cli.init=init.txt --hadoop.cli.verbose=true --hadoop.cli.execute=ls /
```

---

## 4. Session Lifecycle

### 4.1 Session Creation Flow

```
1. HCFSCliApp.main(args)
       |
2. HadoopCliCommandLineOptions.toSpringBootOption(args)
       |  Translate CLI args to Spring --hadoop.cli.* properties
       |
3. SpringApplication.run(HCFSCliApp.class, springArgs)
       |  Component scan discovers Shell, CliEnvironment, CommandRegistry
       |
4. CommandLineRunner beans execute in @Order sequence
       |  Configure verbose, debug, silent, api mode, etc.
       |
5. Shell.startShell()
       |
6. CliEnvironment.createSession("cli")
       |
7. CliSession.builder()
       |  .withConfiguration(config)
       |  .withCredentials(credentials)  // optional
       |  .withCommandRegistry(registry)
       |  .build()
       |
8. CliSession.init()
       |  Check hadoop.security.authentication
       |  If kerberos:
       |    KerberosGlobalState.ensureInitialized()
       |    ugi = credentials.getUGI()  // isolated UGI
       |    ugi.doAs(() -> initializeShellAndFileSystem())
       |  Else:
       |    initializeShellAndFileSystem()
       |
9. initializeShellAndFileSystem()
       |  CliFsShell(config).init()
       |  FileSystemOrganizer().init(config)
       |
10. Shell.acceptCommands(reader, session)
        |  REPL loop: read line -> session.processInput(line)
```

### 4.2 Command Processing Flow

```
User Input: "ls -R /data"
      |
CliSession.processInput(line)
      |  substituteVariables(line)  -- expand ${var} references
      |  Split on | for pipeline support (max depth 1)
      |
CliSession.processCommand(line, cr)
      |  If ugi != null (Kerberos):
      |    ugi.doAs(() -> processCommandInternal(line, cr))
      |    On auth failure: retry up to 7 times with credentials.refresh()
      |  Else:
      |    processCommandInternal(line, cr)
      |
processCommandInternal(line, cr)
      |  Parse arguments (handle quoted strings)
      |  Lookup command in CommandRegistry
      |  Parse options via Commons CLI
      |  command.execute(session, commandLine, cr)
      |
HdfsCommand.implementation(session, cmd, cr)
      |  Build paths via PathBuilder (context-aware)
      |  Construct argv: ["-ls", "-R", "/data"]
      |  ToolRunner.run(shell, argv)
      |  Return CommandReturn with code, stdout, stderr
```

### 4.3 Session State

Each `CliSession` holds:

| Field | Type | Purpose |
|-------|------|---------|
| `hadoopConfig` | `Configuration` | Hadoop XML configuration for this session |
| `credentials` | `SessionCredentials` | Authentication strategy (nullable) |
| `ugi` | `UserGroupInformation` | Per-session isolated UGI (null for simple auth) |
| `shell` | `CliFsShell` | Extended FsShell for native command delegation |
| `fileSystemOrganizer` | `FileSystemOrganizer` | Namespace and working directory tracking |
| `commandRegistry` | `CommandRegistry` | Available commands for this session |
| `properties` | `Properties` | Session-scoped variable substitution |
| `verbose`, `debug`, `silent` | `boolean` | Output control flags |

---

## 5. Authentication Architecture

### 5.1 Overview

The authentication system supports three modes through the `SessionCredentials` interface:

```
SessionCredentials (interface)
    |-- getUGI() -> UserGroupInformation
    |-- refresh() -> void
    |
    +-- DefaultCredentials       Uses current login user (kinit-based)
    +-- KeytabCredentials        Keytab file-based authentication
    +-- ProxyCredentials         Proxy user impersonation (wraps another SessionCredentials)
```

### 5.2 KerberosGlobalState Singleton

`KerberosGlobalState` manages the one-time global initialization required by `UserGroupInformation.setConfiguration()`. This is necessary because:

- `UGI.loginUserFromKeytabAndReturnUGI()` requires `UGI.isSecurityEnabled()` to return true
- `UGI.setConfiguration()` modifies JVM-global state
- Multiple sessions calling `setConfiguration()` with different configs would interfere

The solution: initialize once with a minimal Kerberos-enabled config, then use per-session isolated UGIs.

```
KerberosGlobalState
    +-- static volatile boolean initialized
    +-- static final Object lock
    +-- ensureInitialized()
    |       Double-checked locking
    |       Creates minimal config: hadoop.security.authentication=kerberos
    |       Calls UGI.setConfiguration(kerberosConfig) exactly once
    +-- ensureInitialized(Configuration config)
            Uses provided config if first call
```

### 5.3 Per-Session UGI Isolation

```
Session A (keytab: user1)              Session B (keytab: user2)
    |                                       |
KerberosGlobalState.ensureInitialized()     (already initialized, no-op)
    |                                       |
KeytabCredentials("user1", "/path/a.keytab")  KeytabCredentials("user2", "/path/b.keytab")
    |                                       |
UGI.loginUserFromKeytabAndReturnUGI()       UGI.loginUserFromKeytabAndReturnUGI()
    |  (returns isolated UGI)               |  (returns isolated UGI)
    |                                       |
ugi.doAs(() -> {                            ugi.doAs(() -> {
    initializeShellAndFileSystem()              initializeShellAndFileSystem()
})                                          })
```

### 5.4 Credential Refresh and Retry

During command execution under Kerberos, if authentication fails with "Client cannot authenticate via:[TOKEN, KERBEROS]", the system retries up to 7 times:

1. Execute command via `ugi.doAs()`
2. On auth failure: call `credentials.refresh()` (re-login from keytab)
3. Wait 2 seconds
4. Retry
5. After 7 failures: throw `DisabledException`

---

## 6. Network Architecture

### 6.1 HDFS Communication

HDFS operations use the Hadoop `FileSystem` API and `ToolRunner`:

```
CliSession
    +-- CliFsShell (extends FsShell)
    |       +-- CliCommandFactory (extends CommandFactory)
    |       +-- ToolRunner.run(shell, argv)
    |               |
    |               v
    |       Hadoop FileSystem RPC (IPC protocol)
    |       Port: configured via dfs.namenode.rpc-address (default 8020)
    |
    +-- FileSystemOrganizer
            +-- FileSystem.newInstance(config)  -- DistributedFileSystem
            +-- FileSystem.getLocal(config)    -- LocalFileSystem
```

### 6.2 YARN Resource Manager REST API

YARN statistics commands communicate via HTTP/HTTPS to the Resource Manager REST API:

```
ResourceManagerStats
    |
    +-- getResourceManagerWebAddress()
    |       Check yarn.resourcemanager.ha.enabled
    |       If HA: iterate rm-ids, check /ws/v1/cluster/info for ACTIVE
    |       Return: http(s)://rm-host:port
    |
    +-- getRMState(urlStr)
    |       URL: {rmAddress}/ws/v1/cluster/info
    |       Method: URLConnection.openConnection()
    |       Parse: clusterInfo.haState
    |
    +-- ContainerStats.process(cmd)
    |       URL: {rmAddress}/ws/v1/cluster/apps?{timeRange}
    |       Parse JSON response with Jackson ObjectMapper
    |
    +-- SchedulerStats.process(cmd)
            URL: {rmAddress}/ws/v1/cluster/scheduler
            Parse queue hierarchy recursively
```

### 6.3 NameNode JMX

NameNode statistics are collected via JMX HTTP endpoints:

```
NamenodeJmxParser
    +-- JmxJsonParser (parses JMX JSON response)
    |       Bean: Hadoop:service=NameNode,name=NameNodeStatus
    |       Bean: Hadoop:service=NameNode,name=NameNodeInfo
    |       Bean: Hadoop:service=NameNode,name=FSNamesystemState
    |
    +-- Fields: Version, Used, Free, TotalBlocks, TotalFiles,
    |           CapacityUsed, NumLiveDataNodes, VolumeFailuresTotal, etc.
    |
    +-- TopUserOpCounts parsing (nested JSON within JMX bean)
```

### 6.4 Protocol Summary

| Target | Protocol | Port Configuration | Authentication |
|--------|----------|-------------------|----------------|
| HDFS NameNode | Hadoop IPC (RPC) | `dfs.namenode.rpc-address` | Kerberos / Simple |
| YARN ResourceManager | HTTP/HTTPS REST | `yarn.resourcemanager.webapp.{http\|https}.address` | Kerberos / Simple |
| NameNode JMX | HTTP/HTTPS | NameNode web UI port | Kerberos / Simple |
| Local FileSystem | Local I/O | N/A | OS user |
| Ozone OM | Hadoop IPC (RPC) | `ozone.om.address` | Kerberos / Simple |

---

## 7. File System Abstraction

### 7.1 FileSystemOrganizer

`FileSystemOrganizer` manages all accessible filesystem namespaces in a session. On initialization, it discovers available namespaces from the Hadoop configuration:

```
FileSystemOrganizer.init(config)
    |
    +-- FileSystem.newInstance(config)     -> distributedFileSystem
    +-- FileSystem.getLocal(config)       -> localFileSystem
    |
    +-- Parse dfs.nameservices
    |       For each nameservice:
    |           Create FileSystemState(hdfs://<ns>, /)
    |           Mark default if matches fs.defaultFS
    |
    +-- Parse ozone.om.service.ids / ozone.service.id
    |       For each ozone service:
    |           Create FileSystemState(ofs://<service>, /)
    |
    +-- Create local FileSystemState(file:, cwd)
```

### 7.2 FileSystemState

Each `FileSystemState` tracks:

| Field | Type | Description |
|-------|------|-------------|
| `fileSystem` | `FileSystem` | Hadoop FileSystem instance |
| `namespace` | `String` | Nameservice or service identifier |
| `protocol` | `String` | `hdfs://`, `ofs://`, or `file:` |
| `workingDirectory` | `Path` | Current working directory within this namespace |
| `lastAccessed` | `Date` | Last access timestamp |

### 7.3 Dual Working Directories

The shell maintains independent working directories for:

- **Remote filesystem** (HDFS/Ozone): Tracked per-namespace in `FileSystemState.workingDirectory`
- **Local filesystem**: Tracked in the `LOCAL` `FileSystemState` entry

Commands prefixed with `l` operate on local filesystem (`lls`, `lcd`, `lpwd`, `lcat`, `lhead`, `lmkdir`, `lrm`), while unprefixed commands operate on the current remote filesystem.

### 7.4 Path Resolution

`PathBuilder` resolves relative and absolute paths against the current `FileSystemState`:

- Commands with `Direction.NONE`: Single path, resolved against current namespace
- Commands with `Direction.REMOTE_LOCAL`: Left path from remote, right path from local
- Commands with `Direction.LOCAL_REMOTE`: Left path from local, right path from remote
- Commands with `Direction.REMOTE_REMOTE`: Both paths from remote (cp, mv)

When the current filesystem is not the default, paths are fully qualified with the namespace URI prefix (e.g., `hdfs://ns2/user/data`).

---

## 8. Configuration Loading Chain

### 8.1 Load Priority

```
1. HADOOP_CONF_DIR environment variable
       |
       +-- If set and directory exists:
       |       Load: core-site.xml, hdfs-site.xml, mapred-site.xml,
       |             yarn-site.xml, ozone-site.xml
       |
2. /etc/hadoop/conf (fallback, only when HADOOP_CONF_DIR not set)
       |
       +-- If directory exists: Load same config files
       |
3. Hadoop defaults (built-in Configuration defaults)
       |
4. CliEnvironment.properties overlay
       |  Applied on top of loaded configuration
       |
5. Spring Boot application.yaml
       |  Controls framework behavior (logging, web-type, api mode)
```

### 8.2 Configuration Files

| File | Content |
|------|---------|
| `core-site.xml` | `fs.defaultFS`, security settings, proxy users |
| `hdfs-site.xml` | Nameservices, NameNode addresses, replication settings |
| `mapred-site.xml` | MapReduce framework settings |
| `yarn-site.xml` | ResourceManager addresses, HA config, scheduler settings |
| `ozone-site.xml` | Ozone service IDs, OM addresses |

### 8.3 Per-Session Configuration

Each `CliSession` receives its own `Configuration` instance. The `CliEnvironment.createSession()` method calls `loadDefaultConfiguration()` which creates a fresh `Configuration` object, meaning sessions can have independent Hadoop configurations if created with custom `Configuration` objects via the `createSession(name, config, credentials)` overload.

---

## 9. Build Architecture

### 9.1 Build System

| Property | Value |
|----------|-------|
| Build tool | Apache Maven |
| JDK version | 17 (build-time toolchain) |
| Source compatibility | Java 11 |
| Target compatibility | Java 11 |
| Packaging | JAR (uber-jar via shade plugin) |

### 9.2 Maven Shade Plugin

The project uses `maven-shade-plugin` (3.2.1) to produce a self-contained executable JAR. This bundles all dependencies including the Hadoop client libraries, Spring Boot, and transitive dependencies into a single artifact.

### 9.3 Key Build Properties

```xml
<jdk.version>17</jdk.version>
<maven.compiler.source>11</maven.compiler.source>
<maven.compiler.target>11</maven.compiler.target>
<maven.shade.plugin.version>3.2.1</maven.shade.plugin.version>
```

### 9.4 Dependency Management

The `<dependencyManagement>` section overrides transitive dependencies for security:

- Apache Avro pinned to 1.11.5 (fixes CVE-2025-33042 from hadoop-common transitive)

### 9.5 Exclusion Strategy

Hadoop dependencies carry extensive transitive dependency trees. The build excludes:

- Web server components (Jetty, Jersey) -- not needed for CLI
- Logging bridges (reload4j, slf4j-reload4j) -- replaced by Logback
- Netty components -- not needed for client operations
- Older Jackson versions -- replaced by managed Jackson 2.18.3

---

## 10. Dependency Map

### 10.1 Core Framework

| Dependency | Version | Purpose |
|-----------|---------|---------|
| Spring Boot Starter | 3.4.13 | Application framework, DI, configuration |
| Spring Framework | 6.2.15 | Core, Context, Beans, AOP, Expression, Web |
| Logback Core | 1.5.25 | Logging implementation |
| Logback Classic | 1.5.25 | SLF4J binding for Logback |
| Janino | 3.1.12 | Conditional logging evaluation |

### 10.2 Hadoop Ecosystem

| Dependency | Version | Purpose |
|-----------|---------|---------|
| hadoop-common | 3.4.2 | Core Hadoop APIs, Configuration, FileSystem |
| hadoop-hdfs | 3.4.2 | HDFS client, DistributedFileSystem |
| hadoop-hdfs-client | 3.4.2 | HDFS client protocol |
| ozone-client | 1.4.1 | Apache Ozone filesystem support |

### 10.3 CLI and I/O

| Dependency | Version | Purpose |
|-----------|---------|---------|
| JLine | 2.5 | Interactive console, tab completion, history |
| Commons CLI | 1.9.0 | Command-line option parsing |
| jcabi-manifests | 1.1 | JAR manifest reading for version display |

### 10.4 Data Processing

| Dependency | Version | Purpose |
|-----------|---------|---------|
| Jackson Databind | 2.18.3 | JSON parsing (YARN REST, JMX responses) |
| SnakeYAML | 2.4 | YAML configuration parsing |
| Apache Avro | 1.11.5 | Avro data format support |
| Commons IO | 2.18.0 | I/O utilities (stream to string conversion) |
| Commons Lang3 | 3.18.0 | String utilities, number parsing |
| Commons Text | 1.13.0 | Text processing utilities |
| Commons Pool2 | 2.12.1 | Object pooling |

### 10.5 Development

| Dependency | Version | Scope | Purpose |
|-----------|---------|-------|---------|
| Lombok | 1.18.38 | compile | Annotation-based boilerplate reduction |
| Spring Boot Starter Test | 3.4.13 | test | Test framework |
| Spring Test | 6.2.15 | test | Spring test context |

---

## 11. Architecture Diagrams

### 11.1 System Context Diagram

```
+-------------------+        +---------------------------+
|                   |        |    HCFS Browser CLI        |
|   Administrator / |------->|                           |
|   Data Engineer   |  CLI   |  Spring Boot Application  |
|                   |        |  (non-web, JLine console)  |
+-------------------+        +---------------------------+
                                 |         |          |
                    Hadoop RPC   |    REST |     JMX  |
                    (IPC)        |   HTTP  |    HTTP   |
                                 v         v          v
                          +---------+ +--------+ +---------+
                          |  HDFS   | |  YARN  | |NameNode |
                          |NameNode | |Resource| |  JMX    |
                          |         | |Manager | |         |
                          +---------+ +--------+ +---------+
                                 |
                                 v
                          +-----------+
                          |   Ozone   |
                          |  Manager  |
                          +-----------+
```

### 11.2 Component Diagram

```
+-----------------------------------------------------------------------+
|                         HCFS Browser Application                       |
|                                                                        |
|  +---------------------+     +--------------------------------------+ |
|  |   Spring Boot        |     |          CLI Framework               | |
|  |   Container          |     |                                      | |
|  |                      |     |  +--------+   +------------------+  | |
|  | HCFSCliApp           |---->|  | Shell  |-->| CliEnvironment   |  | |
|  | HadoopCliAppCfg      |     |  | (REPL) |   | (session factory)|  | |
|  | CommandLineOptions   |     |  +--------+   +------------------+  | |
|  +---------------------+     |                  |                    | |
|                               |       +----------+----------+       | |
|                               |       |                     |       | |
|                               |  +----v-----+   +----------v---+   | |
|                               |  |CliSession |   | CliSession   |   | |
|                               |  |  "cli"    |   |  "api-1"     |   | |
|                               |  +----+------+   +--------------+   | |
|                               +-------|------------------------------+ |
|                                       |                                |
|  +------------------------------------v------------------------------+ |
|  |                    Session Components                              | |
|  |                                                                    | |
|  |  +-----------------+  +-------------------+  +------------------+ | |
|  |  | CommandRegistry |  | FileSystem        |  | SessionCredentials|| |
|  |  | (TreeMap)       |  | Organizer         |  | + KerberosGlobal || |
|  |  +-----------------+  +-------------------+  +------------------+ | |
|  |         |                    |                       |             | |
|  |  +------v--------+   +------v--------+    +---------v---------+  | |
|  |  | HDFS Commands |   | FileSystem    |    | UGI (per-session) |  | |
|  |  | YARN Commands |   | State(s)      |    | doAs() wrapping   |  | |
|  |  | Local Commands|   | (per namespace)|    +-------------------+  | |
|  |  | Shell Commands|   +---------------+                            | |
|  |  +---------------+                                                | |
|  +-------------------------------------------------------------------+ |
|                                                                        |
|  +-------------------------------------------------------------------+ |
|  |                  Hadoop Extensions (org.apache.hadoop.fs)          | |
|  |                                                                    | |
|  |  +------------------+    +---------------------+                  | |
|  |  | CliFsShell       |--->| CliCommandFactory   |                  | |
|  |  | (FsShell ext)    |    | (CommandFactory ext) |                  | |
|  |  | I/O redirection  |    | I/O redirection      |                  | |
|  |  +------------------+    +---------------------+                  | |
|  +-------------------------------------------------------------------+ |
+-----------------------------------------------------------------------+
```

### 11.3 Session State Machine

```
                  +----------+
                  |  Created  |
                  +----+-----+
                       |
                  CliSession.init()
                       |
              +--------v---------+
              | Check auth mode  |
              +--------+---------+
              |                  |
         kerberos             simple
              |                  |
    +---------v----------+       |
    |KerberosGlobalState |       |
    |.ensureInitialized()|       |
    +---------+----------+       |
              |                  |
    +---------v----------+       |
    |credentials.getUGI()|       |
    +---------+----------+       |
              |                  |
    +---------v----------+       |
    | ugi.doAs(() ->     |       |
    |   initShellAndFS())|       |
    +---------+----------+       |
              |                  |
              +--------+---------+
                       |
                 initializeShellAndFileSystem()
                       |
              +--------v---------+
              |   Initialized    |
              +--------+---------+
                       |
              +--------v---------+
              | Command Loop     |<----+
              | processInput()   |     |
              +--------+---------+     |
                       |               |
              +--------v---------+     |
              | Auth check       |     |
              | (retry up to 7x) +-----+
              +--------+---------+
                       |
              +--------v---------+
              |  DisabledException|
              |  (auth exhausted) |
              +------------------+
```

### 11.4 Deployment Diagram

```
+-------------------------------------------------------+
|              Client Machine (JDK 17+)                  |
|                                                        |
|  +--------------------------------------------------+ |
|  |  hcfs-browser-5.0.4.0.jar (uber-jar)             | |
|  |                                                    | |
|  |  +----------------------------------------------+ | |
|  |  | JVM Process                                   | | |
|  |  |                                               | | |
|  |  |  Spring Boot 3.4.13 (non-web)                | | |
|  |  |  Hadoop Client 3.4.2                          | | |
|  |  |  Ozone Client 1.4.1                          | | |
|  |  |  JLine 2.5 (console)                         | | |
|  |  +----------------------------------------------+ | |
|  +--------------------------------------------------+ |
|                                                        |
|  +--------------------------------------------------+ |
|  | Configuration Sources                             | |
|  |  $HADOOP_CONF_DIR/ or /etc/hadoop/conf/           | |
|  |    core-site.xml                                  | |
|  |    hdfs-site.xml                                  | |
|  |    yarn-site.xml                                  | |
|  |    mapred-site.xml                                | |
|  |    ozone-site.xml                                 | |
|  +--------------------------------------------------+ |
|                                                        |
|  +--------------------------------------------------+ |
|  | User Data                                         | |
|  |  ~/.hadoop-cli/history   (command history)        | |
|  |  Kerberos keytab files   (authentication)         | |
|  |  Kerberos ticket cache   (kinit)                  | |
|  +--------------------------------------------------+ |
+-------------------------------------------------------+
        |                    |                    |
        | Hadoop RPC         | HTTP/HTTPS         | HTTP/HTTPS
        | (port 8020+)       | (port 8088/8090)   | (JMX port)
        v                    v                    v
+---------------+    +---------------+    +---------------+
| HDFS Cluster  |    | YARN Cluster  |    | NameNode JMX  |
|               |    |               |    |               |
| NameNode(s)   |    | ResourceMgr   |    | Metrics       |
| DataNode(s)   |    | (HA pair)     |    | FSState       |
+---------------+    +---------------+    | TopUserOps    |
                                          +---------------+
        |
        v
+---------------+
| Ozone Cluster |
|               |
| Ozone Manager |
| DataNodes     |
+---------------+
```

---

## 12. Command Registry

The `CommandRegistry` registers all available commands at construction time. The full command set:

### HDFS File Operations

| Command | Class | Direction | Description |
|---------|-------|-----------|-------------|
| `ls` | `HdfsCommand` | NONE | List directory contents |
| `lsr` | `HdfsCommand` | NONE | List recursively |
| `cd` | `HdfsCd` | -- | Change HDFS working directory |
| `pwd` | `HdfsPwd` | -- | Print HDFS working directory |
| `cat` | `HdfsCommand` | NONE | Display file contents |
| `head` | `HdfsCommand` | NONE | Display first bytes |
| `tail` | `HdfsCommand` | NONE | Display last bytes |
| `mkdir` | `HdfsCommand` | NONE | Create directory |
| `rm` | `HdfsCommand` | NONE | Remove files |
| `rmdir` | `HdfsCommand` | NONE | Remove directory |
| `touchz` | `HdfsCommand` | NONE | Create empty file |
| `mv` | `HdfsCommand` | REMOTE_REMOTE | Move/rename |
| `cp` | `HdfsCommand` | REMOTE_REMOTE | Copy within HDFS |
| `get` | `HdfsCommand` | REMOTE_LOCAL | Download from HDFS |
| `put` | `HdfsCommand` | LOCAL_REMOTE | Upload to HDFS |
| `copyFromLocal` | `HdfsCommand` | LOCAL_REMOTE | Upload to HDFS |
| `copyToLocal` | `HdfsCommand` | REMOTE_LOCAL | Download from HDFS |
| `du` | `HdfsCommand` | NONE | Disk usage |
| `df` | `HdfsCommand` | NONE | Filesystem free space |
| `count` | `HdfsCommand` | NONE | Count files and directories |
| `stat` | `HdfsCommand` | NONE | File status |
| `test` | `HdfsCommand` | NONE | Test file attributes |
| `text` | `HdfsCommand` | NONE | Display as text |
| `checksum` | `HdfsCommand` | NONE | File checksum |
| `chown` | `HdfsCommand` | NONE (1 dir) | Change owner |
| `chmod` | `HdfsCommand` | NONE (1 dir) | Change permissions |
| `chgrp` | `HdfsCommand` | NONE (1 dir) | Change group |

### HDFS Snapshot Operations

| Command | Class | Description |
|---------|-------|-------------|
| `allowSnapshot` | `HdfsAllowSnapshot` | Enable snapshots on directory |
| `disallowSnapshot` | `HdfsDisallowSnapshot` | Disable snapshots |
| `lsSnapshottableDir` | `HdfsLsSnapshottableDir` | List snapshottable directories |
| `createSnapshot` | `HdfsCommand` | Create snapshot |
| `deleteSnapshot` | `HdfsCommand` | Delete snapshot |
| `renameSnapshot` | `HdfsCommand` | Rename snapshot |
| `snapshotDiff` | `SnapshotDiff` | Compare snapshots |

### Local File Operations

| Command | Class | Description |
|---------|-------|-------------|
| `lls` | `LocalLs` | Local ls |
| `lcd` | `LocalCd` | Local cd |
| `lpwd` | `LocalPwd` | Local pwd |
| `lcat` | `LocalCat` | Local cat |
| `lhead` | `LocalHead` | Local head |
| `lmkdir` | `LocalMkdir` | Local mkdir |
| `lrm` | `LocalRm` | Local rm |

### Namespace and Connection

| Command | Class | Description |
|---------|-------|-------------|
| `connect` | `HdfsConnect` | Connect to HDFS namespace |
| `use` | `Use` | Switch active namespace |
| `list`/`nss`/`namespaces` | `List` | List available namespaces |

### HDFS Utilities

| Command | Class | Description |
|---------|-------|-------------|
| `lsp` | `HdfsLsPlus` | Extended ls with ACLs and metadata |
| `source` | `HdfsSource` | Execute commands from file |

### YARN Statistics

| Command | Class | Description |
|---------|-------|-------------|
| `cstat` | `ContainerStatsCommand` | Container statistics from RM REST API |
| `sstat` | `SchedulerStatsCommand` | Queue/scheduler stats from RM REST API |

### Shell Commands

| Command | Class | Description |
|---------|-------|-------------|
| `help` | `Help` | Display available commands |
| `history` | `HistoryCmd` | Show command history |
| `env` | `Env` | Display environment properties |
| `exit` | `Exit` | Exit the shell |
