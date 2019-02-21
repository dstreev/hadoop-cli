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

public class LocalCd extends HdfsCommand {
    private Environment env;

    public LocalCd(String name, Environment env) {
        super(name,env);
//        this.env = env;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {

            FileSystem localfs = (FileSystem) env.getValue(Constants.LOCAL_FS);
            String dir = cmd.getArgs().length == 0 ? System
                            .getProperty("user.home") : cmd.getArgs()[0];
            logv(cmd, "Change Dir to: " + dir);
            logv(cmd, "CWD: " + localfs.getWorkingDirectory());
            Path newPath = null;
            if (dir.startsWith("~/")) {
                dir = System.getProperty("user.home") + dir.substring(1);
            }
            logv(cmd,"Dir: " + dir);
            newPath = new Path(dir);

            Path qPath = localfs.makeQualified(newPath);
            logv(cmd, "Qual Path: " + qPath);

            if (localfs.getFileStatus(qPath).isDir() && localfs.exists(qPath)) {
                localfs.setWorkingDirectory(qPath);
            }
            else {
                log(cmd, "No such directory: " + dir);
            }
            FSUtil.prompt(env);
        }
        catch (IOException e) {
            log(cmd, e.getMessage());
        }

    }

    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, true);
    }

}
