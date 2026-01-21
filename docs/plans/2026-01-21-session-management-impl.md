# Session Management Revamp Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Revamp CliEnvironment and CliSession to properly handle session creation with lookup-before-create, default configuration loading, and conditional UGI based on security mode.

**Architecture:** CliEnvironment is the Spring singleton factory for CliSession instances. Sessions are cached by name. Configuration loading follows priority: HADOOP_CONF_DIR env → /etc/hadoop/conf → Hadoop defaults. UGI is only used when `hadoop.security.authentication=kerberos`.

**Tech Stack:** Java 17, Hadoop 3.x, Spring Boot, Lombok

---

## Task 1: Update CliSession Builder - Default Credentials to Null

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/cli/CliSession.java:311-318`

**Step 1: Change default credentials from DefaultCredentials to null**

In the `CliSessionBuilder` class, change line 313:

```java
// Before:
private SessionCredentials credentials = new DefaultCredentials();

// After:
private SessionCredentials credentials = null;
```

**Step 2: Compile to verify**

Run: `mvn compile -q`
Expected: BUILD SUCCESS (init() will fail at runtime but compiles)

**Step 3: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/CliSession.java
git commit -m "refactor: default CliSession credentials to null

Credentials now default to null in builder. The init() method will
determine UGI behavior based on security mode configuration.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Update CliSession.init() for Conditional UGI

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/cli/CliSession.java:75-90`

**Step 1: Rewrite init() method with security mode check**

Replace the `init()` method (lines 75-90) with:

```java
public void init() throws IOException {
    String authMode = hadoopConfig.get("hadoop.security.authentication", "simple");
    boolean kerberosEnabled = "kerberos".equalsIgnoreCase(authMode);

    if (kerberosEnabled) {
        // Kerberos mode - use UGI
        if (credentials != null) {
            this.ugi = credentials.getUGI();
        } else {
            UserGroupInformation.setConfiguration(hadoopConfig);
            this.ugi = UserGroupInformation.getLoginUser();
        }

        try {
            ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                initializeShellAndFileSystem();
                return null;
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Initialization interrupted", e);
        }
    } else {
        // Simple auth - no UGI wrapping
        this.ugi = null;
        initializeShellAndFileSystem();
    }
}

private void initializeShellAndFileSystem() throws IOException {
    this.shell = new CliFsShell(hadoopConfig);
    this.shell.init();
    this.fileSystemOrganizer = new FileSystemOrganizer();
    this.fileSystemOrganizer.init(hadoopConfig);
}
```

**Step 2: Compile to verify**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/CliSession.java
git commit -m "feat: conditional UGI in CliSession based on security mode

- Check hadoop.security.authentication property
- Kerberos mode: use UGI (from credentials or login user)
- Simple mode: skip UGI wrapping entirely
- Extract initializeShellAndFileSystem() helper method

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Update CliSession.processCommand() for Conditional UGI

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/cli/CliSession.java:154-164`

**Step 1: Update processCommand to conditionally use doAs**

Replace the `processCommand()` method (lines 154-164) with:

```java
public CommandReturn processCommand(String line, CommandReturn commandReturn) throws DisabledException {
    if (ugi != null) {
        // Kerberos mode - wrap in doAs
        try {
            return ugi.doAs((PrivilegedExceptionAction<CommandReturn>) () ->
                processCommandInternal(line, commandReturn));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DisabledException("Command interrupted");
        } catch (IOException e) {
            throw new DisabledException("IO error: " + e.getMessage());
        }
    } else {
        // Simple mode - execute directly
        return processCommandInternal(line, commandReturn);
    }
}
```

**Step 2: Compile to verify**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/CliSession.java
git commit -m "feat: conditional doAs in processCommand based on UGI presence

When UGI is null (simple auth mode), commands execute directly
without doAs wrapping.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Add loadDefaultConfiguration() to CliEnvironment

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java`

**Step 1: Add loadDefaultConfiguration() method after line 142**

Add this method after the `init()` method:

```java
/**
 * Load Hadoop configuration from environment with fallback.
 * Priority: HADOOP_CONF_DIR env → /etc/hadoop/conf → Hadoop defaults
 */
private Configuration loadDefaultConfiguration() {
    Configuration config = new Configuration(true);

    String hadoopConfDir = System.getenv(HADOOP_CONF_DIR);

    if (hadoopConfDir != null) {
        File confDir = new File(hadoopConfDir).getAbsoluteFile();
        if (confDir.exists() && confDir.isDirectory()) {
            log.info("Loading Hadoop configuration from HADOOP_CONF_DIR: {}", hadoopConfDir);
            loadConfigFilesFromDir(config, confDir);
        } else {
            log.warn("HADOOP_CONF_DIR set to '{}' but directory does not exist. Using Hadoop defaults.", hadoopConfDir);
        }
    } else {
        File defaultConfDir = new File("/etc/hadoop/conf").getAbsoluteFile();
        if (defaultConfDir.exists() && defaultConfDir.isDirectory()) {
            log.info("Loading Hadoop configuration from /etc/hadoop/conf");
            loadConfigFilesFromDir(config, defaultConfDir);
        } else {
            log.info("No Hadoop configuration directory found. Using Hadoop defaults.");
        }
    }

    // Apply any properties set on CliEnvironment
    getProperties().stringPropertyNames().forEach(k -> {
        config.set(k, getProperties().getProperty(k));
    });

    return config;
}

private void loadConfigFilesFromDir(Configuration config, File confDir) {
    for (String file : HADOOP_CONF_FILES) {
        File f = new File(confDir, file);
        if (f.exists()) {
            log.debug("Adding configuration resource: {}", f.getAbsolutePath());
            config.addResource(new org.apache.hadoop.fs.Path(f.getAbsolutePath()));
        }
    }
}
```

**Step 2: Compile to verify**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java
git commit -m "feat: add loadDefaultConfiguration() with fallback logic

Configuration loading priority:
1. HADOOP_CONF_DIR environment variable (if exists)
2. /etc/hadoop/conf (if exists)
3. Hadoop built-in defaults

Logs warnings when expected directories don't exist.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Add createSession(String name) Method

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java:148-150`

**Step 1: Add new createSession overload before existing createSession methods**

Insert before line 148:

```java
/**
 * Create a session with default configuration loaded from environment.
 * If session already exists, returns existing session.
 */
public CliSession createSession(String name) throws IOException {
    // Check for existing session first
    CliSession existing = sessions.get(name);
    if (existing != null) {
        log.debug("Returning existing session: {}", name);
        return existing;
    }

    Configuration config = loadDefaultConfiguration();
    return createSession(name, config, null);
}
```

**Step 2: Update existing createSession(name, config) to pass null credentials**

Change lines 148-150 from:

```java
public CliSession createSession(String name, Configuration config) throws IOException {
    return createSession(name, config, new DefaultCredentials());
}
```

To:

```java
public CliSession createSession(String name, Configuration config) throws IOException {
    return createSession(name, config, null);
}
```

**Step 3: Compile to verify**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java
git commit -m "feat: add createSession(String name) with default config loading

New method loads configuration from environment (HADOOP_CONF_DIR or
/etc/hadoop/conf) and creates session with null credentials.
Existing createSession(name, config) now passes null credentials.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Add getOrCreateSession() Convenience Method

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java`

**Step 1: Add getOrCreateSession() after getSession()**

Insert after the `getSession()` method (around line 175):

```java
/**
 * Get existing session by name, or create with defaults if not found.
 */
public CliSession getOrCreateSession(String name) throws IOException {
    CliSession session = sessions.get(name);
    if (session == null) {
        session = createSession(name);
    }
    return session;
}
```

**Step 2: Compile to verify**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java
git commit -m "feat: add getOrCreateSession() convenience method

Returns existing session if found, otherwise creates new session
with default configuration.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 7: Simplify CliEnvironment.init() Method

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java:97-142`

**Step 1: Simplify init() to use new methods**

Replace the `init()` method with:

```java
public synchronized void init() {
    if (!isDisabled() && !isInitialized()) {
        try {
            log.info("Initializing CliEnvironment");

            // Load and cache default configuration
            this.defaultHadoopConfig = loadDefaultConfiguration();

            // Set up Kerberos user property if applicable
            String authMode = defaultHadoopConfig.get("hadoop.security.authentication", "simple");
            if ("kerberos".equalsIgnoreCase(authMode)) {
                UserGroupInformation.setConfiguration(defaultHadoopConfig);
                getProperties().setProperty(CURRENT_USER_PROP, UserGroupInformation.getCurrentUser().getShortUserName());
            }

            // Create default session
            createSession(defaultSessionName, defaultHadoopConfig, null);

            setInitialized(Boolean.TRUE);
            log.info("CliEnvironment initialized successfully");

        } catch (IOException e) {
            log.error("Failed to initialize CliEnvironment: {}", e.getMessage());
        }
    }
}
```

**Step 2: Compile to verify**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

**Step 3: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java
git commit -m "refactor: simplify init() to use loadDefaultConfiguration()

Removed duplicate config loading code. init() now:
- Uses loadDefaultConfiguration() for config
- Sets Kerberos user property when applicable
- Creates default session with null credentials

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 8: Remove DefaultCredentials Import and Clean Up

**Files:**
- Modify: `src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java`

**Step 1: Remove unused DefaultCredentials import**

Remove line 19:
```java
import com.cloudera.utils.hadoop.cli.session.DefaultCredentials;
```

**Step 2: Compile to verify**

Run: `mvn compile -q`
Expected: BUILD SUCCESS

**Step 3: Full build verification**

Run: `mvn clean package -DskipTests -q && echo "BUILD SUCCESS"`
Expected: BUILD SUCCESS

**Step 4: Commit**

```bash
git add src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java
git commit -m "chore: remove unused DefaultCredentials import

CliEnvironment no longer creates DefaultCredentials directly;
credentials are now optional (null) and CliSession handles
UGI based on security mode.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Summary

**Total Tasks:** 8

**Files Modified:**
- `src/main/java/com/cloudera/utils/hadoop/cli/CliSession.java` (Tasks 1-3)
- `src/main/java/com/cloudera/utils/hadoop/cli/CliEnvironment.java` (Tasks 4-8)

**Key Changes:**
1. CliSession builder defaults credentials to null
2. CliSession.init() checks security mode for conditional UGI
3. CliSession.processCommand() conditionally uses doAs
4. CliEnvironment.loadDefaultConfiguration() with fallback logic
5. CliEnvironment.createSession(String name) loads default config
6. CliEnvironment.getOrCreateSession() convenience method
7. Simplified CliEnvironment.init()
8. Removed unused import
