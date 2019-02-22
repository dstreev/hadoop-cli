package com.streever.hadoop.hdfs.shell.command;

import jline.console.ConsoleReader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.streever.tools.stemshell.Environment;

/**
 * Created by streever on 2015-11-22.
 */

public class LocalRm extends HdfsCommand {
    private boolean local = false;

    public LocalRm(String name, boolean local) {
        super(name);
        this.local = local;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {
            FileSystem hdfs = this.local ? (FileSystem) env.getValue(Constants.LOCAL_FS)
                            : (FileSystem) env.getValue(Constants.HDFS);
            String remoteFile = cmd.getArgs()[0];

            logv(env, "HDFS file: " + remoteFile);
            Path hdfsPath = new Path(hdfs.getWorkingDirectory(), remoteFile);
            logv(env, "Remote path: " + hdfsPath);

            boolean recursive = cmd.hasOption("r");
            logv(env, "Deleting recursively...");
            hdfs.delete(hdfsPath, recursive);

            FSUtil.prompt(env);

        }
        catch (Throwable e) {
            log(env, "Error: " + e.getMessage());
        }

    }

    @Override
    public Options getOptions() {
        Options opts =  super.getOptions();
        opts.addOption("r", false, "delete recursively");
        return opts;
    }
}
