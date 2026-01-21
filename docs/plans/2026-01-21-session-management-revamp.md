# Session Management Revamp Design

**Date:** 2026-01-21
**Goal:** Revamp CliEnvironment and CliSession to properly handle session creation, configuration loading, and credential management.

---

## Overview

CliEnvironment is the Spring singleton that acts as a factory and registry for CliSession instances. This design clarifies:
- Session lookup before creation (avoid duplicates)
- Default configuration loading from environment
- Conditional UGI usage based on security mode

---

## Session Creation API

### CliEnvironment Methods

```java
// Create session with just a name - loads default config from environment
public CliSession createSession(String name) throws IOException

// Create session with name and custom config (no explicit credentials)
public CliSession createSession(String name, Configuration config) throws IOException

// Create session with name, config, and explicit credentials
public CliSession createSession(String name, Configuration config, SessionCredentials credentials) throws IOException

// Get existing session, or create with defaults if not found
public CliSession getOrCreateSession(String name) throws IOException
```

### Session Lookup

All `createSession` methods check for existing session first:

```java
CliSession existing = sessions.get(name);
if (existing != null) {
    return existing;
}
// ... create new session
```

---

## Configuration Loading

For `createSession(String name)`, configuration loads with this priority:

1. **HADOOP_CONF_DIR environment variable** - If set and directory exists
2. **/etc/hadoop/conf** - If HADOOP_CONF_DIR not set and directory exists
3. **Hadoop defaults** - Built-in Configuration defaults

### Fallback Behavior

- If HADOOP_CONF_DIR is set but directory doesn't exist: log warning, use Hadoop defaults
- If /etc/hadoop/conf doesn't exist: log warning, use Hadoop defaults

### Config Files Loaded

From the config directory (when found): `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`

### Implementation

```java
private Configuration loadDefaultConfiguration() {
    Configuration config = new Configuration(true);  // Load Hadoop defaults

    String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");

    if (hadoopConfDir != null) {
        File confDir = new File(hadoopConfDir);
        if (confDir.exists() && confDir.isDirectory()) {
            loadConfigFilesFromDir(config, confDir);
        } else {
            log.warn("HADOOP_CONF_DIR set to '{}' but directory does not exist. Using Hadoop defaults.", hadoopConfDir);
        }
    } else {
        File defaultConfDir = new File("/etc/hadoop/conf");
        if (defaultConfDir.exists() && defaultConfDir.isDirectory()) {
            loadConfigFilesFromDir(config, defaultConfDir);
        } else {
            log.warn("No Hadoop configuration found at /etc/hadoop/conf. Using Hadoop defaults.");
        }
    }

    return config;
}
```

---

## Credentials and UGI Handling

### Security Mode Detection

Check `hadoop.security.authentication` property in configuration:
- `"kerberos"` → Kerberos mode, use UGI
- `"simple"` or unset → Simple mode, no UGI wrapping

### CliSession.init() Behavior

```java
public void init() throws IOException {
    String authMode = hadoopConfig.get("hadoop.security.authentication", "simple");
    boolean kerberosEnabled = "kerberos".equalsIgnoreCase(authMode);

    if (kerberosEnabled) {
        if (credentials != null) {
            this.ugi = credentials.getUGI();
        } else {
            UserGroupInformation.setConfiguration(hadoopConfig);
            this.ugi = UserGroupInformation.getLoginUser();
        }

        ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            initializeShellAndFileSystem();
            return null;
        });
    } else {
        this.ugi = null;
        initializeShellAndFileSystem();
    }
}
```

### Command Execution

```java
public CommandReturn processCommand(String line, CommandReturn commandReturn) throws DisabledException {
    if (ugi != null) {
        return ugi.doAs(() -> processCommandInternal(line, commandReturn));
    } else {
        return processCommandInternal(line, commandReturn);
    }
}
```

### Behavior Matrix

| Scenario | Security Mode | Credentials | UGI Behavior |
|----------|---------------|-------------|--------------|
| createSession("name") | simple | null | No UGI |
| createSession("name") | kerberos | null | Login user UGI |
| createSession("name", config, creds) | kerberos | KeytabCredentials | Keytab UGI |

---

## CliSession Builder Changes

Credentials default to `null` instead of `DefaultCredentials`:

```java
public static class CliSessionBuilder {
    private Configuration configuration;
    private SessionCredentials credentials = null;  // null by default
    // ...

    public CliSession build() throws IOException {
        if (configuration == null) {
            throw new IllegalStateException("Configuration is required");
        }

        CliSession session = new CliSession();
        session.setHadoopConfig(configuration);
        session.setCredentials(credentials);  // May be null
        // ...
        session.init();  // Handles null credentials based on security mode

        return session;
    }
}
```

---

## Files to Modify

1. **CliEnvironment.java**
   - Add `createSession(String name)`
   - Add `getOrCreateSession(String name)`
   - Add `loadDefaultConfiguration()` private method
   - Simplify `init()` method

2. **CliSession.java**
   - Update `init()` for conditional UGI based on security mode
   - Update `processCommand()` for conditional doAs
   - Update builder to default credentials to `null`

---

## Implementation Notes

- Existing sessions are never recreated - lookup returns cached instance
- Configuration is immutable once session is created
- UGI refresh handled by SessionCredentials.refresh() when needed