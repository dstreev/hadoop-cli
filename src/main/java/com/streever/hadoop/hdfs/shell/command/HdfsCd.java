// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.streever.hadoop.hdfs.shell.command;

import java.io.IOException;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.streever.hadoop.hdfs.shell.completers.FileSystemNameCompleter;
import com.streever.tools.stemshell.Environment;

public class HdfsCd extends HdfsCommand {
    private Environment env;

    public HdfsCd(String name, Environment env) {
        super(name, env);
        this.env = env;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        FileSystem hdfs = null;
        try {
            hdfs = (FileSystem) env.getValue(Constants.HDFS);

            String dir = cmd.getArgs().length == 0 ? "/" : cmd.getArgs()[0];
            logv(env, "CWD before: " + hdfs.getWorkingDirectory());
            logv(env, "Requested CWD: " + dir);

            Path newPath = null;
            if (dir.startsWith("/")) {
                newPath = new Path(env.getProperty(Constants.HDFS_URL), dir);
            } else {
                newPath = new Path(hdfs.getWorkingDirectory(), dir);
            }

            Path qPath = newPath.makeQualified(hdfs);
            logv(env, "" + newPath);
            if (hdfs.getFileStatus(qPath).isDir() && hdfs.exists(qPath)) {
                hdfs.setWorkingDirectory(qPath);
            } else {
                log(env, "No such directory: " + dir);
            }

        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {
            FSUtil.prompt(env);
        }
    }

    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, false);
    }

}
