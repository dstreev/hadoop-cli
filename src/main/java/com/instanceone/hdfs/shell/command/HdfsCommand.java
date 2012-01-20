// Copyright (c) 2011 Health Market Science, Inc.

package com.instanceone.hdfs.shell.command;

import java.io.IOException;
import java.net.URI;

import jline.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.AbstractCommand;
import com.instanceone.hdfs.shell.Environment;

public abstract class HdfsCommand extends AbstractCommand {
    public static final String HDFS_URL = "hdfs.url";
    public static final String HDFS_CWD = "hdfs.cwd";

    public HdfsCommand(String name) {
        super(name);
    }

    protected String hdfsUrl(Environment env, ConsoleReader reader)
                    throws IOException {
        String hdfsUrl = env.getProperty(HDFS_URL);
        if (hdfsUrl == null) {
            hdfsUrl = reader.readLine("Enter HDFS URL: ");
            env.setProperty(HDFS_URL, hdfsUrl);
        }
        return hdfsUrl;
    }

    protected String cwd(Environment env, ConsoleReader reader) throws IOException {
        hdfsUrl(env, reader);
        String cwd = env.getProperty(HDFS_CWD);
        if (cwd == null) {
            cwd = "/";
            env.setProperty(HDFS_CWD, cwd);
        }
        return cwd;
    }

    protected FileSystem getFileSystem(Environment env, ConsoleReader reader)
                    throws IOException {
        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(hdfsUrl(env, reader)),
                        config);
        return hdfs;
    }

}
