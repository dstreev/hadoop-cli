package com.streever.hadoop.hdfs.util;

import com.streever.hadoop.hdfs.shell.command.HdfsConnect;
import com.streever.hadoop.shell.Environment;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileSystemState {
    private static String DISTRIBUTED_USER_HOME_BASE = "/user";

    private boolean partOfConfig = Boolean.FALSE;
    private FileSystem fileSystem = null;
    private String namespace = null;
    private String protocol = null;
    private Path workingDirectory = null;
    private Date lastAccessed = null;

    public FileSystemState() {

    }

    public FileSystemState(String uri) {
        // Break up uri into components;

    }

    public FileSystemState(String uri, FileSystem fileSystem) {
        // Break up uri into components;

    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    public void setFileSystem(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    public boolean isPartOfConfig() {
        return partOfConfig;
    }

    public void setPartOfConfig(boolean partOfConfig) {
        this.partOfConfig = partOfConfig;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getHomeDir(Environment environment) {
        StringBuilder sb = new StringBuilder();
        String userName = environment.getProperties().getProperty(HdfsConnect.CURRENT_USER_PROP, System.getProperty("user.name"));
        if (fileSystem instanceof DistributedFileSystem) {
            return DISTRIBUTED_USER_HOME_BASE + "/" + userName;
        } else {
            String homeDir = System.getProperty("user.home");
            return homeDir;
        }
    }

    public String getURI() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.protocol);
        if (this.namespace != null) {
            sb.append(this.namespace);
        }
        return sb.toString();
    }

    public Path getWorkingDirectory() {
        return workingDirectory;
    }

    public void setWorkingDirectory(Path workingDirectory) {
        // Strip the URI from the incoming path, if exists
        this.workingDirectory = new Path(workingDirectory.toString().replace(getURI(), ""));
//        this.workingDirectory = workingDirectory;
    }

    public Date getLastAccessed() {
        return lastAccessed;
    }

    public void setLastAccessed(Date lastAccessed) {
        this.lastAccessed = lastAccessed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileSystemState that = (FileSystemState) o;

        if (namespace != null ? !namespace.equals(that.namespace) : that.namespace != null) return false;
        return protocol != null ? protocol.equals(that.protocol) : that.protocol == null;
    }

    @Override
    public int hashCode() {
        int result = namespace != null ? namespace.hashCode() : 0;
        result = 31 * result + (protocol != null ? protocol.hashCode() : 0);
        return result;
    }

    public String toShortDisplay() {
        StringBuilder sb = new StringBuilder();
        sb.append(protocol);
        sb.append(namespace);
        return sb.toString();
    }

    public String toDisplay() {
        StringBuilder sb = new StringBuilder();
        sb.append(getURI());
        if (workingDirectory != null) {
//            sb.append("(");
            sb.append(workingDirectory);
//            sb.append(")");
        }
        return sb.toString();
    }

}
