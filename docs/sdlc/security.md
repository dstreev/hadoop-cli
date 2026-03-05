# Security Document -- HCFS Browser

**Version:** 5.0.4.0
**Last Updated:** 2026-03-04

---

## 1. Overview

HCFS Browser is a CLI and API tool for interacting with Hadoop Compatible File Systems (HDFS, Ozone, cloud storage). It operates as a client-side application that authenticates to Hadoop clusters using the cluster's native security mechanisms. The tool does not implement its own authentication or authorization layer; it delegates entirely to Hadoop's security infrastructure.

---

## 2. Authentication Mechanisms

### 2.1 Authentication Modes

The tool supports two authentication modes, determined by the Hadoop configuration property `hadoop.security.authentication`:

- **Simple authentication** -- No Kerberos. The tool runs as the OS-level user. UGI is not used.
- **Kerberos authentication** -- Full Kerberos integration via Hadoop's `UserGroupInformation` (UGI) framework.

The mode is detected at session initialization time in `CliSession.init()` (`CliSession.java:77-78`):

```java
String authMode = hadoopConfig.get("hadoop.security.authentication", "simple");
boolean kerberosEnabled = "kerberos".equalsIgnoreCase(authMode);
```

### 2.2 Credential Types

Three credential strategies are implemented via the `SessionCredentials` interface (`SessionCredentials.java`):

| Credential Type | Class | Description |
|---|---|---|
| **Default** | `DefaultCredentials` | Uses the current user's existing Kerberos ticket (from `kinit`) via `UserGroupInformation.getCurrentUser()` |
| **Keytab** | `KeytabCredentials` | Authenticates using a Kerberos principal and keytab file via `loginUserFromKeytabAndReturnUGI()` |
| **Proxy** | `ProxyCredentials` | Impersonates another user via `UserGroupInformation.createProxyUser()`, backed by real credentials |

### 2.3 Keytab Authentication

`KeytabCredentials` (`KeytabCredentials.java:55-62`) performs keytab login:

```java
public synchronized UserGroupInformation getUGI() throws IOException {
    if (ugi == null) {
        ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
    }
    return ugi;
}
```

Key properties:
- Uses `loginUserFromKeytabAndReturnUGI()` which returns an **isolated** UGI instance, avoiding global state pollution.
- UGI is lazily initialized on first access.
- The `getUGI()` method is `synchronized` for thread safety.
- Keytab path is accepted as a constructor parameter; no keytab paths are hardcoded.

### 2.4 Proxy User (Impersonation)

`ProxyCredentials` (`ProxyCredentials.java:56-63`) creates a proxy user backed by real credentials:

```java
public synchronized UserGroupInformation getUGI() throws IOException {
    if (proxyUgi == null) {
        UserGroupInformation realUgi = realCredentials.getUGI();
        proxyUgi = UserGroupInformation.createProxyUser(proxyUser, realUgi);
    }
    return proxyUgi;
}
```

Proxy user authorization is enforced server-side by Hadoop's `hadoop.proxyuser.*` configuration. The tool itself performs no proxy user authorization checks -- this is by design, as authorization is the cluster's responsibility.

---

## 3. Credential Lifecycle

### 3.1 Lazy Initialization

All credential types use lazy initialization. The UGI is not created until `getUGI()` is first called, which occurs during `CliSession.init()` (`CliSession.java:89`).

### 3.2 Credential Refresh

Each credential type implements `refresh()`:

- **KeytabCredentials** (`KeytabCredentials.java:71-79`): Calls `ugi.checkTGTAndReloginFromKeytab()` to renew the TGT if expired.
- **DefaultCredentials** (`DefaultCredentials.java:50-53`): Calls `checkTGTAndReloginFromKeytab()` on the current user (no-op if not keytab-based).
- **ProxyCredentials** (`ProxyCredentials.java:73-78`): Refreshes the underlying real credentials and invalidates the proxy UGI so it is recreated on next use.

### 3.3 Retry on Authentication Failure

`CliSession.processCommand()` (`CliSession.java:192-208`) implements retry logic for Kerberos authentication failures:

```java
int attempts = 0;
while (attempts < 7) {
    attempts++;
    check = ugi.doAs(() -> processCommandInternal(line, commandReturn));
    if (check.isError() && check.getReturn().contains(
            "Client cannot authenticate via:[TOKEN, KERBEROS]")) {
        if (credentials != null) {
            credentials.refresh();
        } else {
            ugi.checkTGTAndReloginFromKeytab();
        }
        Thread.sleep(2000);
    } else {
        return check;
    }
}
```

- Maximum of 7 retry attempts with 2-second delays between retries.
- On each retry, credentials are explicitly refreshed before reattempting.
- After exhausting retries, a `DisabledException` is thrown with a clear error message (`CliSession.java:211-212`).
- The retry targets a specific error string (`Client cannot authenticate via:[TOKEN, KERBEROS]`), preventing retries on unrelated failures.

---

## 4. Session Isolation

### 4.1 Per-Session UGI

Each `CliSession` maintains its own `UserGroupInformation` instance (`CliSession.java:56`). Sessions are stored in a `ConcurrentHashMap` within `CliEnvironment` (`CliEnvironment.java:86`), providing thread-safe session management.

### 4.2 Global Kerberos State Management

`KerberosGlobalState` (`KerberosGlobalState.java`) manages the one-time global initialization required by `UserGroupInformation.setConfiguration()`:

- Uses double-checked locking with a `volatile` flag (`KerberosGlobalState.java:52`) for thread-safe, idempotent initialization.
- Only called once per JVM lifetime. Subsequent calls are no-ops.
- Creates a minimal `Configuration` with only `hadoop.security.authentication=kerberos`, avoiding pollution from session-specific configurations.

### 4.3 doAs Execution

All commands in a Kerberos session execute within `ugi.doAs()` blocks (`CliSession.java:99-102`, `CliSession.java:194-195`), ensuring that filesystem operations and YARN API calls run under the correct authenticated identity.

`CliFsShell.init()` (`CliFsShell.java:72-88`) explicitly avoids calling `UserGroupInformation.setConfiguration()` per-session, as documented in the code comments. This prevents one session's configuration from overwriting another's global state.

---

## 5. Authorization Model

HCFS Browser does not implement its own authorization layer. All authorization is delegated to the Hadoop cluster:

- **HDFS**: File and directory access is governed by HDFS permissions (POSIX-style), ACLs, and Ranger/Sentry policies on the cluster.
- **YARN**: REST API access to ResourceManager is governed by the cluster's YARN ACLs and security configuration.
- **Proxy user**: Impersonation is authorized by the cluster's `hadoop.proxyuser.<superuser>.hosts` and `hadoop.proxyuser.<superuser>.groups` settings.

The tool acts as the authenticated user. It has no elevated privileges beyond what the user's Kerberos principal or proxy user configuration grants on the cluster.

---

## 6. Network Security

### 6.1 HDFS Communication

HDFS communication uses the Hadoop RPC protocol with SASL authentication when Kerberos is enabled. Data transfer encryption and RPC encryption are controlled by cluster-side configuration properties (`dfs.encrypt.data.transfer`, `hadoop.rpc.protection`). The tool inherits these settings from the loaded Hadoop configuration.

### 6.2 YARN REST API Connections

The YARN statistics classes (`ResourceManagerStats.java`, `SchedulerStats.java`, `ContainerStats.java`, `HdfsNNStats.java`) make HTTP/HTTPS connections to cluster REST APIs using `java.net.URLConnection`:

- **Protocol selection**: Controlled by a user-specified `-ssl` flag (`ResourceManagerStats.java:234-239`). When SSL is enabled, the tool uses `https://` and reads from `yarn.resourcemanager.webapp.https.address.*` configuration properties.
- **TLS/SSL verification**: Relies on the JVM's default `TrustManager` and truststore (`javax.net.ssl.trustStore`). No certificate verification is bypassed or disabled.
- **No custom HTTP clients**: The tool uses `java.net.URL.openConnection()` directly, inheriting standard JVM SSL behavior.

Note: `HdfsNNStats` (`HdfsNNStats.java:157-169`) constructs URLs using `http://` when connecting to NameNode JMX endpoints. The class is marked `@Deprecated`.

### 6.3 No Outbound Connections to External Services

The tool does not phone home, check for updates, or transmit telemetry. All network connections are to Hadoop cluster endpoints specified in the user's Hadoop configuration files.

---

## 7. Data Handling

### 7.1 No Credential Storage

- No credentials (passwords, keytab contents, Kerberos tickets) are stored by the application.
- Keytab file paths are held in memory only for the duration of the session.
- No credential caching beyond what Hadoop's UGI provides natively.

### 7.2 No Hardcoded Secrets

- No API keys, passwords, or secrets are present in the source code.
- No default credentials are shipped with the application.
- Configuration is loaded entirely from the user's Hadoop configuration directory (`HADOOP_CONF_DIR` or `/etc/hadoop/conf`).

### 7.3 In-Memory Data

- YARN statistics data (application records, queue stats, NN info) is held in memory in `LinkedHashMap` structures and cleared via `clearCache()`.
- No sensitive data is written to temporary files.

---

## 8. Input Validation and Sanitization

### 8.1 Command Parsing

User input is parsed using regex-based argument splitting (`CliSession.java:262-274`, `CliEnvironment.java:325-338`):

```java
Pattern regex = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'");
```

This handles quoted strings (both single and double quotes) but passes arguments directly to Hadoop `FsShell` commands. Path traversal and special characters are handled by the underlying Hadoop filesystem implementation.

### 8.2 Variable Substitution

`CliSession.substituteVariables()` (`CliSession.java:288-318`) performs variable expansion using `${var}` and `$var` syntax:

- Variables are resolved from `System.getProperty()` first, then from session `Properties`.
- Replacement values are properly escaped via `Matcher.quoteReplacement()` (`CliSession.java:305`) to prevent regex injection.
- No environment variable (`System.getenv()`) expansion is performed in the substitution logic.

### 8.3 Script Execution

The `runFile()` method (`CliSession.java:321-365`, `CliEnvironment.java:389-469`) executes commands from local files:

- Lines starting with `#` are treated as comments and skipped.
- Empty lines are skipped.
- File paths can be absolute or relative to the local working directory.
- No shell command injection is possible; lines are processed through the same command registry as interactive input.

### 8.4 Pipeline Support

Command pipelining (`CliSession.java:151-157`) is limited to single-depth pipes (maximum 2 commands). Multi-level pipelines are rejected with an error message.

---

## 9. Logging Security

### 9.1 Logging Configuration

Logging is configured via Logback (`logback-hcfs-cli.xml`):

- **Log location**: `${user.home}/.hcfs-cli/logs/` by default, configurable via `LOGGING_DIR`.
- **Retention**: 30-day history, 100MB max file size, 1GB total cap.
- **Default threshold**: `WARN` level (configurable via `LOGGING_THRESHOLD`).
- Log files are written with the JVM process owner's permissions.

### 9.2 Sensitive Data in Logs

Review of logging statements across the codebase:

- **Keytab paths** are logged at `INFO` level in `KeytabCredentials.java:57`: `"Logging in from keytab: principal={}, keytab={}"`. This logs the principal name and keytab file path, which is acceptable for operational diagnostics. No keytab file contents are logged.
- **Proxy user names** are logged at `INFO` level in `ProxyCredentials.java:59`.
- **User names** from UGI are logged at `DEBUG` level in multiple locations.
- **File paths** being executed are logged at `INFO`/`DEBUG` level.
- **Command content** (user-entered commands) is logged at `DEBUG` level (`CliSession.java:348`).
- **No passwords, tokens, or credential material** are logged.
- **YARN REST API URLs** (including hostnames) are logged at `DEBUG` level.

### 9.3 Considerations

- At `DEBUG` or `TRACE` level, filesystem URIs and working directories are logged (`CliSession.java:129-130`). In production, the default `WARN` threshold prevents this.
- The console appender logs to stdout, which could be captured by process supervision systems.

---

## 10. Configuration Security

### 10.1 Hadoop Configuration Loading

Configuration is loaded in `CliEnvironment.loadDefaultConfiguration()` (`CliEnvironment.java:127-156`):

1. `HADOOP_CONF_DIR` environment variable (if set and valid).
2. `/etc/hadoop/conf` fallback (if exists).
3. Hadoop built-in defaults.

Standard Hadoop configuration files loaded: `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`, `ozone-site.xml`.

### 10.2 Hadoop Credential Providers

The tool supports Hadoop credential providers (`hadoop.security.credential.provider.path`) through standard Hadoop `Configuration` loading. Sensitive values (such as SSL keystore passwords) can be stored in JCEKS credential stores rather than in plaintext XML files. This is inherited behavior from the Hadoop `Configuration` class -- no additional code is required in the tool.

### 10.3 Spring Configuration

`application.yaml` configures:
- Web application type: `NONE` (no HTTP server is started).
- Banner mode: `off`.
- API mode: disabled by default.

The application does not expose any HTTP endpoints or web interfaces by default.

---

## 11. Dependency Security

### 11.1 Direct Dependencies

| Dependency | Version | Notes |
|---|---|---|
| Spring Boot | 3.4.13 | No web server started (`web-application-type: NONE`) |
| Spring Framework | 6.2.15 | Core DI framework |
| Hadoop (common, hdfs, hdfs-client) | 3.4.2 | Core Hadoop libraries |
| Jackson | 2.18.3 | JSON processing for YARN REST API responses |
| Logback | 1.5.25 | Logging framework |
| Apache Commons (lang3, io, cli, text) | Current versions | Utility libraries |
| Apache Avro | 1.11.5 | Override of transitive dependency (see CVE notes) |
| Lombok | 1.18.38 | Compile-time only annotation processor |
| SnakeYAML | 2.4 | YAML parsing |

### 11.2 Known Vulnerabilities

#### jackson-core 2.18.3 -- GHSA-72hv-8253-57qq

- **Severity**: Low (DoS)
- **Description**: Potential denial of service via crafted JSON input causing excessive memory allocation.
- **Risk for this project**: Low. Jackson is used only to parse JSON responses from trusted YARN ResourceManager REST APIs (`ResourceManagerStats.java`, `SchedulerStats.java`). The tool does not parse untrusted JSON input from external sources.

#### Transitive Dependencies from Hadoop 3.4.2

Hadoop 3.4.2 brings in several transitive dependencies with known vulnerabilities. The project mitigates these through exclusions where possible:

| Dependency | Version | Scope | Notes |
|---|---|---|---|
| protobuf-java | 2.5.0 | Transitive (Hadoop) | Legacy version required by Hadoop's internal RPC serialization. Not upgradeable without Hadoop version change. |
| guava | 27.0-jre | Transitive (Hadoop) | Used internally by Hadoop. Excluded or shaded in the Hadoop distribution. |
| okhttp | 4.9.x | Transitive (Hadoop) | Used by Hadoop for HTTP client operations. |

The `pom.xml` applies extensive exclusions to Hadoop dependencies (Jetty, Jersey, Netty, reload4j, javax.servlet) to reduce the transitive dependency surface. Avro is explicitly overridden via `dependencyManagement` to version 1.11.5 to address CVE-2025-33042.

### 11.3 Dependency Management Practices

- Jackson databind is excluded from Hadoop transitive dependencies and managed directly at version 2.18.3.
- Jetty server components are excluded (the tool does not serve HTTP).
- Jersey components are excluded.
- Netty is excluded from hadoop-hdfs.
- The `cdp.scope` property defaults to `compile`, allowing deployment with a pre-installed Hadoop environment (where Hadoop jars would be `provided`).

---

## 12. Secure Coding Practices

### 12.1 Thread Safety

- `KeytabCredentials.getUGI()` and `ProxyCredentials.getUGI()` are `synchronized`.
- `KerberosGlobalState` uses double-checked locking with `volatile`.
- Session map in `CliEnvironment` uses `ConcurrentHashMap`.

### 12.2 Resource Management

- `InterruptedException` handling restores the interrupt flag (`Thread.currentThread().interrupt()`) in `CliSession.java:104` and `CliSession.java:213`.
- `CliFsShell` sets quiet mode on the configuration (`CliFsShell.java:73`) to prevent Hadoop from writing directly to console output streams.

### 12.3 Error Handling

- Authentication failures produce structured `DisabledException` with descriptive messages rather than exposing stack traces to the user.
- Caught exceptions in command processing are logged but do not leak internal state to the user output stream.
- The `Throwable` catch in `CliSession.processCommandInternal()` (`CliSession.java:249`) is intentionally broad to prevent any command from crashing the session.

---

## 13. Areas for Improvement

### 13.1 HTTP Connection Security

- `HdfsNNStats` (deprecated) hardcodes `http://` for NameNode JMX connections (`HdfsNNStats.java:157-169`). Consider removing or updating to support HTTPS.
- YARN REST API connections via `URLConnection` do not set connection or read timeouts, which could result in indefinite hangs if a cluster endpoint is unresponsive.
- No SPNEGO (Kerberos HTTP) authentication is performed on YARN REST API connections. In a Kerberos-secured cluster, these API calls may fail or fall back to simple authentication depending on cluster configuration.

### 13.2 Input Handling

- Variable substitution resolves `System.getProperty()` values, which could expose JVM system properties if an attacker controls input templates. In practice, this is limited to interactive CLI input from the authenticated user.
- The broad `Throwable` catch in `processCommandInternal()` (`CliSession.java:249`) silently swallows errors including `OutOfMemoryError`. Consider catching `Exception` separately from `Error`.

### 13.3 Logging

- At `DEBUG` level, the principal name and keytab path are logged. While useful for troubleshooting, ensure log files have appropriate filesystem permissions in production deployments.

### 13.4 Dependency Updates

- Monitor Hadoop transitive dependencies (protobuf-java, guava) for security updates. These can only be updated when the Hadoop dependency itself is updated.
- Continue the practice of excluding unused transitive dependencies to minimize attack surface.

---

## 14. Security Checklist Summary

| Control | Status | Notes |
|---|---|---|
| Authentication | Implemented | Kerberos (keytab, kinit, proxy user) via Hadoop UGI |
| Authorization | Delegated | Enforced by Hadoop cluster (HDFS ACLs, YARN ACLs, Ranger) |
| Credential storage | None | No credentials persisted by the application |
| TLS/SSL | Supported | YARN REST APIs support `-ssl` flag; HDFS uses Hadoop RPC encryption |
| Session isolation | Implemented | Per-session UGI with isolated Kerberos state |
| Input validation | Basic | Regex-based parsing; path validation delegated to Hadoop |
| Logging | Configured | No credentials logged; configurable log levels |
| Hardcoded secrets | None | No secrets in source code |
| Web attack surface | None | No HTTP server (`web-application-type: NONE`) |
| Dependency management | Active | Exclusions applied; CVE-2025-33042 mitigated via Avro override |
