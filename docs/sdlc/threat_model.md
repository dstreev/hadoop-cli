# Threat Model -- HCFS Browser

**Version:** 5.0.4.0
**Date:** 2026-03-04
**Document Status:** Initial Draft

---

## 1. Introduction

This document presents a threat model for the HCFS Browser application, a CLI and API tool for interacting with Hadoop Compatible File Systems (HDFS, Ozone, S3A) and YARN cluster management services. The analysis identifies threats, assesses risk, documents existing mitigations, and recommends additional controls.

## 2. Methodology

This threat model uses the **STRIDE** framework developed by Microsoft:

| Category | Description |
|---|---|
| **S**poofing | An attacker pretends to be another user or system |
| **T**ampering | Unauthorized modification of data or code |
| **R**epudiation | Ability to deny performing an action without proof |
| **I**nformation Disclosure | Exposure of information to unauthorized parties |
| **D**enial of Service | Making the system unavailable or degraded |
| **E**levation of Privilege | Gaining capabilities beyond what is authorized |

Each threat is assigned a risk rating based on **Likelihood x Impact** using the following scales:

- **Likelihood:** Low (1), Medium (2), High (3)
- **Impact:** Low (1), Medium (2), High (3), Critical (4)
- **Risk = Likelihood x Impact** -- Low (1-3), Medium (4-6), High (7-9), Critical (10-12)

## 3. System Overview

### 3.1 System Description

HCFS Browser is a Java CLI application (Spring Boot, non-web) that provides:

- Interactive shell for HDFS/Ozone/S3A file operations (ls, cd, put, get, rm, chmod, chown, etc.)
- Script execution from files via `-f` and `-i` flags
- YARN Resource Manager statistics collection via REST APIs
- HDFS NameNode JMX statistics collection
- Multi-session management with independent Kerberos credentials
- Proxy user impersonation support

### 3.2 Data Flow Diagram

```
+---------------------+          +-----------------------------+
|   User Terminal /    |  stdin   |     HCFS Browser JVM        |
|   Calling Process    |--------->|  (Spring Boot, non-web)     |
|                      |<---------|                             |
|                      |  stdout  |  +---------------------+   |
+---------------------+          |  | CliEnvironment       |   |
                                  |  |  +---------------+  |   |
+---------------------+          |  |  | CliSession(s)  |  |   |
| Local Filesystem    |<-------->|  |  | - UGI          |  |   |
| - Script files      |  R/W    |  |  | - FsShell      |  |   |
| - Config files      |          |  |  | - Credentials  |  |   |
| - Keytab files      |          |  |  +---------------+  |   |
| - Log files         |          |  +---------------------+   |
+---------------------+          +-------|---------|----------+
                                          |         |
                        Kerberos (GSSAPI) |         | RPC / REST
                                          v         v
                                  +-------+   +-------------+
                                  |  KDC  |   | Hadoop      |
                                  |       |   | Cluster     |
                                  +-------+   | - NameNode  |
                                              | - DataNode  |
                                              | - YARN RM   |
                                              +-------------+
```

### 3.3 Trust Boundaries

```
TB1: User Terminal <---> CLI JVM Process
     (CLI input, script files, environment variables)

TB2: CLI JVM Process <---> Local Filesystem
     (keytab files, config files, log output, script files)

TB3: CLI JVM Process <---> Hadoop Cluster
     (HDFS RPC, YARN REST API, NameNode JMX)

TB4: CLI JVM Process <---> Kerberos KDC
     (TGT acquisition, ticket renewal via GSSAPI)
```

### 3.4 Assets

| Asset | Sensitivity | Location |
|---|---|---|
| Kerberos keytab files | Critical | Local filesystem |
| Kerberos TGT (in-memory) | Critical | JVM heap |
| Kerberos principal names | High | Configuration, logs |
| HDFS data | High | Hadoop cluster (accessed via RPC) |
| Hadoop configuration files (core-site.xml, hdfs-site.xml, yarn-site.xml) | High | Local filesystem, HADOOP_CONF_DIR |
| Session state (UGI objects) | High | JVM heap |
| JMX/REST API responses | Medium | In-memory, potentially logged |
| CLI command history | Medium | JVM memory |
| Log files | Medium | Local filesystem |
| Application properties | Low | Classpath, local filesystem |

## 4. Attack Surface Analysis

### 4.1 CLI Input (TB1)

- **Entry point:** Interactive command line input, piped stdin
- **Processing:** `CliSession.processInput()` (line 147) parses commands with regex, performs variable substitution via `substituteVariables()`, supports single-depth pipelining via `|` delimiter
- **Variable substitution:** Resolves `${var}` and `$var` patterns from `System.getProperty()` and session properties (lines 288-318 in CliSession.java)
- **Risk:** Command injection through crafted variable values, though substitution uses `Matcher.quoteReplacement()` for safe replacement

### 4.2 Script Files (TB2)

- **Entry point:** `-f` (source file) and `-i` (init file) command-line flags invoke `CliSession.runFile()` (line 321)
- **Processing:** Reads lines from local files, trims whitespace, skips comments (`#`), applies `MessageFormat` template substitution, then executes each line via `processInput()`
- **Path resolution:** Supports absolute paths and relative paths resolved against local working directory (lines 325-335)
- **Risk:** Malicious script files execute arbitrary CLI commands with the user's credentials; no integrity verification on script files

### 4.3 Network Connections -- YARN REST API (TB3)

- **Classes:** `ResourceManagerStats`, `ContainerStats`, `SchedulerStats`
- **Protocol:** HTTP or HTTPS based on `ssl` flag; URLs constructed from `yarn.resourcemanager.webapp.{http|https}.address` configuration
- **Connection:** `java.net.URLConnection.openConnection()` with no explicit timeout configuration
- **Data format:** JSON parsed via Jackson `ObjectMapper`
- **Risk:** No connection/read timeouts -- connections can hang indefinitely; server responses parsed without size limits

### 4.4 Network Connections -- HDFS NameNode JMX (TB3)

- **Class:** `HdfsNNStats` (deprecated but present)
- **Protocol:** HTTP only (hardcoded `http://` in `getNamenodeHTTPUrls()`, line 157)
- **Connection:** `java.net.URLConnection` with no timeouts
- **Data format:** JSON via `IOUtils.toString()` with no size limits
- **Risk:** Same timeout and unbounded read issues as YARN connections; HTTP-only means data in transit is unencrypted

### 4.5 Configuration Files (TB2)

- **Loading:** `CliEnvironment.loadDefaultConfiguration()` loads from `HADOOP_CONF_DIR` environment variable or falls back to `/etc/hadoop/conf`
- **Files loaded:** `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, and other standard Hadoop configuration files
- **Risk:** Manipulated configuration files can redirect authentication to rogue KDC, point HDFS/YARN connections to attacker-controlled hosts, or alter security settings

### 4.6 Dependencies

Key dependency versions from `pom.xml`:

| Dependency | Version | Notes |
|---|---|---|
| Jackson (databind, core, annotations) | 2.18.3 | JSON parsing for REST/JMX responses |
| commons-lang3 | 3.18.0 | General utilities |
| commons-io | 2.18.0 | Stream-to-string conversion for network responses |
| commons-cli | 1.9.0 | Command-line parsing |
| Hadoop | 3.4.2 | Core HDFS/YARN client libraries |
| Spring Boot | 3.4.13 | Application framework (non-web mode) |
| Avro | 1.11.5 | Overridden to address CVE-2025-33042 |
| SnakeYAML | 2.4 | YAML configuration parsing |
| Logback | 1.5.25 | Logging framework |

## 5. STRIDE Threat Analysis

### 5.1 Spoofing

#### T-S1: Keytab File Theft

**Description:** An attacker with access to the local filesystem steals keytab files referenced by `KeytabCredentials` to impersonate the legitimate user. The keytab path is passed directly to `UserGroupInformation.loginUserFromKeytabAndReturnUGI()` (KeytabCredentials.java:58) without validation.

- **Likelihood:** Medium (2) -- requires local filesystem access
- **Impact:** Critical (4) -- full identity impersonation, access to all HDFS data and cluster resources
- **Risk:** High (8)
- **Existing Mitigations:** Keytab files are typically protected by OS file permissions; Kerberos ticket expiration limits window
- **Recommended Mitigations:**
  - Validate keytab file permissions at load time (warn if group/world readable)
  - Log keytab usage with principal and source IP for audit trail

#### T-S2: Proxy User Impersonation Abuse

**Description:** `ProxyCredentials` (ProxyCredentials.java:56-63) allows creating a proxy user identity via `UserGroupInformation.createProxyUser()`. If an attacker controls the `proxyUser` parameter, they can impersonate any user that the real user is authorized to proxy as.

- **Likelihood:** Low (1) -- requires control of session creation parameters
- **Impact:** Critical (4) -- act as arbitrary users within proxy authorization scope
- **Risk:** Medium (4)
- **Existing Mitigations:** Hadoop enforces proxy user authorization rules server-side (hadoop.proxyuser.*.hosts, hadoop.proxyuser.*.groups)
- **Recommended Mitigations:**
  - Log proxy user creation with both real and proxy user identities
  - Consider restricting proxy user names to a configurable allowlist

#### T-S3: Configuration File Manipulation to Redirect Authentication

**Description:** An attacker who can modify `HADOOP_CONF_DIR` environment variable or the configuration files it points to can redirect Kerberos authentication to a rogue KDC, or point HDFS/YARN connections to attacker-controlled services. The `loadDefaultConfiguration()` method (CliEnvironment.java:127-156) trusts whatever directory is specified.

- **Likelihood:** Medium (2) -- requires environment variable or filesystem write access
- **Impact:** Critical (4) -- credential theft, data exfiltration via rogue services
- **Risk:** High (8)
- **Existing Mitigations:** OS-level access controls on environment and filesystem
- **Recommended Mitigations:**
  - Log the resolved configuration directory at startup
  - Warn if configuration directory permissions are overly permissive

### 5.2 Tampering

#### T-T1: Malicious Script Injection via -f / -i Flags

**Description:** The `runFile()` method (CliSession.java:321-365) reads commands from a local file and executes them line by line via `processInput()`. There is no integrity check (signature, checksum) on script files. An attacker who can write to the script file location can inject arbitrary commands that execute with the user's Hadoop credentials.

- **Likelihood:** Medium (2) -- requires write access to script file directory
- **Impact:** High (3) -- arbitrary file operations on HDFS, data modification/deletion
- **Risk:** Medium (6)
- **Existing Mitigations:** OS file permissions on script directories
- **Recommended Mitigations:**
  - Log the full path of script files before execution
  - Consider an option to verify script file ownership matches the running user

#### T-T2: Variable Injection via System Properties

**Description:** `substituteVariables()` (CliSession.java:288-318) resolves `${var}` and `$var` patterns by first checking `System.getProperty()` then session properties. An attacker who can set JVM system properties (e.g., via `-D` flags on the command line or through a parent process) can inject values into any command that uses variable substitution.

- **Likelihood:** Low (1) -- requires control of JVM startup parameters
- **Impact:** Medium (2) -- could alter paths, hostnames, or other parameters in commands
- **Risk:** Low (2)
- **Existing Mitigations:** `Matcher.quoteReplacement()` is used to safely escape replacement strings
- **Recommended Mitigations:**
  - Document the variable resolution order for operators
  - Consider namespace-prefixing for application-specific variables

#### T-T3: Configuration File Tampering

**Description:** Hadoop XML configuration files loaded by `loadConfigFilesFromDir()` (CliEnvironment.java:158-166) are trusted without integrity verification. A modified `core-site.xml` could change `hadoop.security.authentication` from `kerberos` to `simple`, bypassing Kerberos entirely.

- **Likelihood:** Medium (2) -- requires filesystem write access
- **Impact:** High (3) -- security downgrade, connecting to wrong cluster
- **Risk:** Medium (6)
- **Existing Mitigations:** OS-level file permissions
- **Recommended Mitigations:**
  - Log the effective authentication mode at session creation
  - Warn when authentication mode is `simple` in production environments

### 5.3 Repudiation

#### T-R1: Limited Audit Logging for Privileged Operations

**Description:** The application uses SLF4J/Logback for logging but does not produce a structured audit trail. Privileged operations such as proxy user creation, keytab authentication, file operations on HDFS, and session management are logged at DEBUG/INFO level but not in a tamper-evident or centralized audit format. An operator could perform destructive HDFS operations and the only record would be in local log files that the same operator controls.

- **Likelihood:** Medium (2) -- any authenticated user can perform operations
- **Impact:** Medium (2) -- inability to trace who performed what action and when
- **Risk:** Medium (4)
- **Existing Mitigations:** HDFS and YARN have their own server-side audit logs; Logback logging captures some operation details
- **Recommended Mitigations:**
  - Add structured audit logging for credential operations (keytab login, proxy user creation)
  - Log all commands executed through `processInput()` at a dedicated audit level
  - Consider syslog or centralized log output for audit events

### 5.4 Information Disclosure

#### T-I1: JMX Data Exposure

**Description:** `HdfsNNStats` (HdfsNNStats.java:84-140) and YARN stats classes query JMX endpoints and NameNode/ResourceManager REST APIs that return detailed cluster metadata including: top user operations, filesystem state, capacity information, application details (users, queues, tracking URLs, diagnostics), and container resource usage. This data is stored in-memory and potentially output to stdout or HDFS.

- **Likelihood:** Medium (2) -- any user running stats commands
- **Impact:** Medium (2) -- cluster operational details useful for reconnaissance
- **Risk:** Medium (4)
- **Existing Mitigations:** Access to JMX/REST endpoints may be ACL-protected on the cluster side
- **Recommended Mitigations:**
  - Document which stats commands expose sensitive operational data
  - Ensure HTTPS is used for JMX/REST connections (HdfsNNStats currently hardcodes HTTP)

#### T-I2: Log File Contents with Sensitive Data

**Description:** Log statements throughout the codebase include principal names (KeytabCredentials.java:57 `"Logging in from keytab: principal={}, keytab={}"`) and keytab file paths. If log files are accessible to other users on the system, this reveals credential details.

- **Likelihood:** Medium (2) -- log files often have relaxed permissions
- **Impact:** High (3) -- exposes principal names and keytab locations for T-S1
- **Risk:** Medium (6)
- **Existing Mitigations:** Log level configuration can suppress DEBUG messages
- **Recommended Mitigations:**
  - Redact keytab paths in log output, or log only at TRACE level
  - Ensure log file permissions are restrictive (owner-only read/write)
  - Review logback configuration for log rotation and access controls

#### T-I3: Error Message Information Leakage

**Description:** Exception messages from Hadoop operations are logged and may be returned in `CommandReturn` objects. These can include hostnames, port numbers, filesystem paths, principal names, and stack traces that aid an attacker in reconnaissance.

- **Likelihood:** High (3) -- errors occur during normal operation
- **Impact:** Low (1) -- provides supplementary information for other attacks
- **Risk:** Low (3)
- **Existing Mitigations:** None specific
- **Recommended Mitigations:**
  - Sanitize error messages returned to non-debug output
  - Reserve full stack traces for DEBUG/TRACE log levels

### 5.5 Denial of Service

#### T-D1: URLConnection Without Timeouts

**Description:** All network connections in `ResourceManagerStats.getRMState()` (line 287), `ContainerStats.execute()` (line 115), `SchedulerStats.execute()` (line 140), and `HdfsNNStats.process()` (lines 92, 98) use `URLConnection.openConnection()` and `getInputStream()` without setting connect or read timeouts. If a target host is unreachable or slow, the thread blocks indefinitely.

- **Likelihood:** Medium (2) -- network issues are common in distributed environments
- **Impact:** High (3) -- application hangs indefinitely, requires forced termination
- **Risk:** Medium (6)
- **Existing Mitigations:** None
- **Recommended Mitigations:**
  - Set explicit connect and read timeouts on all `URLConnection` instances (e.g., 30 seconds)
  - Example: `connection.setConnectTimeout(30000); connection.setReadTimeout(30000);`

#### T-D2: Kerberos Authentication Retry Loop Amplification

**Description:** `CliSession.processCommand()` (lines 192-208) retries up to 7 times with a 2-second sleep when Kerberos authentication fails, totaling a minimum 14-second delay per command. If multiple commands in a script file each trigger retries, execution time grows linearly: a 100-line script could block for over 23 minutes.

- **Likelihood:** Medium (2) -- Kerberos failures occur due to clock skew, expired tickets, KDC outages
- **Impact:** Medium (2) -- delayed execution, unresponsive application during retry window
- **Risk:** Medium (4)
- **Existing Mitigations:** Retry count is bounded at 7; `DisabledException` is thrown after exhaustion
- **Recommended Mitigations:**
  - Make retry count and sleep interval configurable
  - Implement exponential backoff instead of fixed 2-second sleeps
  - Add a circuit breaker: fail fast on subsequent commands if authentication has already failed N times

#### T-D3: Unbounded Response Parsing

**Description:** `IOUtils.toString(connection.getInputStream())` is used in `ContainerStats.execute()`, `SchedulerStats.execute()`, `HdfsNNStats.process()`, and `ResourceManagerStats.getRMState()` to read entire HTTP responses into memory as a single String. A malicious or misconfigured server could return an extremely large response, causing `OutOfMemoryError`.

- **Likelihood:** Low (1) -- requires compromised or misconfigured cluster service
- **Impact:** High (3) -- JVM crash, complete application failure
- **Risk:** Low (3)
- **Existing Mitigations:** None
- **Recommended Mitigations:**
  - Use a bounded read with a maximum response size (e.g., 10 MB)
  - Use Jackson streaming API instead of reading entire response into memory

#### T-D4: Jackson Async Parser Denial of Service (GHSA-72hv-8253-57qq)

**Description:** Jackson versions prior to certain patch levels are vulnerable to DoS via crafted async parsing input. The project uses Jackson 2.18.3 which should include fixes for known CVEs, but the use of `ObjectMapper.readValue()` on untrusted network input (REST API responses) remains a concern if future vulnerabilities are discovered.

- **Likelihood:** Low (1) -- requires crafted input from a compromised service
- **Impact:** Medium (2) -- parser hang or excessive CPU consumption
- **Risk:** Low (2)
- **Existing Mitigations:** Jackson 2.18.3 includes patches for known async parser issues
- **Recommended Mitigations:**
  - Keep Jackson dependencies up to date
  - Configure `DeserializationFeature.FAIL_ON_TRAILING_TOKENS` to reject malformed input
  - Consider using `ObjectMapper` with `StreamReadConstraints` for input size limits

#### T-D5: commons-lang3 Recursive StackOverflow

**Description:** Certain versions of commons-lang3 are susceptible to StackOverflow via deeply nested input patterns. The project uses commons-lang3 3.18.0 primarily through `NumberUtils.isCreatable()` in `ResourceManagerStats.init()` (line 162).

- **Likelihood:** Low (1) -- requires crafted CLI input reaching NumberUtils
- **Impact:** Low (1) -- JVM crash of the CLI process
- **Risk:** Low (1)
- **Existing Mitigations:** commons-lang3 3.18.0 is a recent version with known fixes
- **Recommended Mitigations:**
  - Keep dependencies current
  - Input validation before passing to library functions

### 5.6 Elevation of Privilege

#### T-E1: Proxy User Impersonation Without Client-Side Controls

**Description:** `ProxyCredentials` (ProxyCredentials.java:42-44) accepts any `proxyUser` string and any `SessionCredentials` as the real user. There is no client-side validation that the calling user should be allowed to proxy as the requested user. While Hadoop enforces proxy rules server-side, the client happily constructs the proxy UGI for any user string.

- **Likelihood:** Low (1) -- proxy user creation requires API-level access; Hadoop enforces server-side
- **Impact:** High (3) -- if server-side rules are misconfigured, arbitrary user impersonation
- **Risk:** Low (3)
- **Existing Mitigations:** Hadoop server-side `hadoop.proxyuser.*` ACL enforcement
- **Recommended Mitigations:**
  - Add client-side logging of all proxy user creation attempts
  - Document proxy user security model for operators

#### T-E2: No Path Validation in File Operations

**Description:** `runFile()` in both `CliEnvironment` (lines 389-470) and `CliSession` (lines 321-365) resolves file paths from user input without sanitization. Relative paths are resolved against the local working directory. While this is expected behavior for a CLI tool, it means any path accessible to the running user can be read and its contents executed as commands.

- **Likelihood:** Medium (2) -- the tool is designed to run files, but path traversal could be unexpected
- **Impact:** Medium (2) -- execution of unintended script files
- **Risk:** Medium (4)
- **Existing Mitigations:** OS file permissions limit accessible files; script content is processed as CLI commands, not arbitrary shell commands
- **Recommended Mitigations:**
  - Log the resolved absolute path before execution
  - Consider an option to restrict script execution to specific directories

#### T-E3: Configuration Manipulation to Downgrade Security

**Description:** By modifying `HADOOP_CONF_DIR` to point to an attacker-controlled directory, or by replacing `core-site.xml`, an attacker can set `hadoop.security.authentication=simple`, causing `CliSession.init()` (lines 77-78) to skip Kerberos entirely. Operations would proceed with simple authentication, potentially gaining access that would normally require Kerberos.

- **Likelihood:** Low (1) -- requires environment or filesystem control
- **Impact:** Critical (4) -- complete bypass of Kerberos authentication
- **Risk:** Medium (4)
- **Existing Mitigations:** The Hadoop cluster must also accept simple auth for this to succeed; environment variable control requires process-level access
- **Recommended Mitigations:**
  - Log a warning when authentication mode is `simple` in an environment where Kerberos was expected
  - Allow a command-line flag to enforce Kerberos-only mode

## 6. Risk Matrix Summary

| Threat ID | Threat | Category | Likelihood | Impact | Risk Score | Risk Level |
|---|---|---|---|---|---|---|
| T-S1 | Keytab file theft | Spoofing | Medium (2) | Critical (4) | 8 | High |
| T-S2 | Proxy user impersonation abuse | Spoofing | Low (1) | Critical (4) | 4 | Medium |
| T-S3 | Config file redirect authentication | Spoofing | Medium (2) | Critical (4) | 8 | High |
| T-T1 | Malicious script injection | Tampering | Medium (2) | High (3) | 6 | Medium |
| T-T2 | Variable injection via system properties | Tampering | Low (1) | Medium (2) | 2 | Low |
| T-T3 | Configuration file tampering | Tampering | Medium (2) | High (3) | 6 | Medium |
| T-R1 | Limited audit logging | Repudiation | Medium (2) | Medium (2) | 4 | Medium |
| T-I1 | JMX data exposure | Info Disclosure | Medium (2) | Medium (2) | 4 | Medium |
| T-I2 | Log file sensitive data | Info Disclosure | Medium (2) | High (3) | 6 | Medium |
| T-I3 | Error message leakage | Info Disclosure | High (3) | Low (1) | 3 | Low |
| T-D1 | URLConnection without timeouts | DoS | Medium (2) | High (3) | 6 | Medium |
| T-D2 | Retry loop amplification | DoS | Medium (2) | Medium (2) | 4 | Medium |
| T-D3 | Unbounded response parsing | DoS | Low (1) | High (3) | 3 | Low |
| T-D4 | Jackson async parser DoS | DoS | Low (1) | Medium (2) | 2 | Low |
| T-D5 | commons-lang3 StackOverflow | DoS | Low (1) | Low (1) | 1 | Low |
| T-E1 | Proxy user without client controls | EoP | Low (1) | High (3) | 3 | Low |
| T-E2 | No path validation in file operations | EoP | Medium (2) | Medium (2) | 4 | Medium |
| T-E3 | Config manipulation to downgrade security | EoP | Low (1) | Critical (4) | 4 | Medium |

## 7. Existing Mitigations Summary

| Control | Threats Addressed |
|---|---|
| Hadoop server-side proxy user ACLs | T-S2, T-E1 |
| Kerberos ticket expiration and renewal | T-S1 |
| OS file permissions on keytab, config, scripts | T-S1, T-S3, T-T1, T-T3 |
| `Matcher.quoteReplacement()` in variable substitution | T-T2 |
| Retry count bounded at 7 attempts | T-D2 |
| HDFS/YARN server-side audit logging | T-R1 |
| Jackson 2.18.3 with known CVE patches | T-D4 |
| Avro version override for CVE-2025-33042 | Dependency vulnerability |
| `KerberosGlobalState` one-time initialization (double-checked locking) | Session isolation |
| Isolated UGI per session via `loginUserFromKeytabAndReturnUGI()` | Session isolation |
| Spring Boot `web-application-type: NONE` | No HTTP attack surface |

## 8. Recommended Mitigations -- Priority Order

### High Priority

1. **Set network timeouts on all URLConnection instances** (T-D1)
   - Add `setConnectTimeout()` and `setReadTimeout()` to all `URLConnection` usage in `ResourceManagerStats`, `ContainerStats`, `SchedulerStats`, and `HdfsNNStats`
   - Suggested default: 30 seconds for both connect and read

2. **Validate keytab file permissions** (T-S1)
   - On `KeytabCredentials` initialization, check that the keytab file is not group/world readable
   - Log a warning if permissions are too permissive

3. **Redact sensitive data in log output** (T-I2)
   - Avoid logging full keytab paths at INFO level
   - Use TRACE level for credential-related details

### Medium Priority

4. **Add structured audit logging** (T-R1)
   - Log all `processInput()` invocations with timestamp, session name, authenticated principal, and command text
   - Log proxy user creation and keytab authentication events

5. **Bound HTTP response size** (T-D3)
   - Replace `IOUtils.toString(inputStream)` with a bounded read utility
   - Reject responses larger than a configurable maximum (e.g., 10 MB)

6. **Enable HTTPS for NameNode JMX** (T-I1)
   - Add SSL support to `HdfsNNStats.getNamenodeHTTPUrls()` (currently hardcoded to HTTP)
   - Respect the same `ssl` flag used by YARN stats classes

7. **Configurable retry parameters** (T-D2)
   - Make retry count and sleep interval configurable via properties
   - Implement exponential backoff

### Low Priority

8. **Log resolved configuration directory at startup** (T-S3, T-T3, T-E3)
   - Emit a startup log line showing the effective `HADOOP_CONF_DIR` and authentication mode
   - Warn if authentication mode is `simple`

9. **Log script file paths before execution** (T-T1, T-E2)
   - Log the resolved absolute path in `runFile()` at INFO level

10. **Keep dependencies up to date** (T-D4, T-D5)
    - Regularly review and update Jackson, commons-lang3, Hadoop, and other transitive dependencies
    - Run `mvn dependency:tree` and check against known CVE databases

## 9. Out of Scope

The following are outside the scope of this threat model:

- **Hadoop cluster security configuration** -- Server-side ACLs, Ranger policies, and cluster-level security settings are managed by cluster administrators
- **Network-level attacks** -- Man-in-the-middle attacks on Kerberos or HDFS RPC are mitigated by Kerberos mutual authentication and SASL/TLS configuration at the Hadoop level
- **Physical security** -- Physical access to machines running the CLI
- **Operating system vulnerabilities** -- OS-level security is a separate concern
- **Supply chain attacks on Maven Central** -- Dependency integrity is assumed to be managed by the build pipeline

## 10. Review Schedule

This threat model should be reviewed:

- When new network-facing features are added
- When authentication or credential handling logic changes
- When major dependency versions are upgraded
- At minimum annually
