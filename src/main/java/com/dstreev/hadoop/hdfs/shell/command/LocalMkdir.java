package com.dstreev.hadoop.hdfs.shell.command;

import java.io.IOException;

import com.instanceone.hdfs.shell.command.FSUtil;
import com.instanceone.hdfs.shell.command.HdfsCommand;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.instanceone.hdfs.shell.completers.FileSystemNameCompleter;
import com.instanceone.stemshell.Environment;

/**
 * Created by dstreev on 2015-11-22.
 */

public class LocalMkdir extends HdfsCommand {

    public static final int LINE_COUNT = 10;

    private Environment env;
    private boolean local = false;

    public LocalMkdir(String name, Environment env, boolean local) {
        super(name, env);
//        this.env = env;
        this.local = local;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader console) {
        FileSystem hdfs = this.local ? (FileSystem) env.getValue(Constants.LOCAL_FS)
                        : (FileSystem) env.getValue(Constants.HDFS);
        logv(cmd, "CWD: " + hdfs.getWorkingDirectory());

        if (cmd.getArgs().length == 1) {
            Path path = new Path(hdfs.getWorkingDirectory(), cmd.getArgs()[0]);

            try {
                logv(cmd, "Create directory: " + path);
                hdfs.mkdirs(path);

            }
            catch (IOException e) {
                log(cmd, "Error creating directory '" + cmd.getArgs()[0]
                                + "': " + e.getMessage());
            }
        }
        else {
        }
        FSUtil.prompt(env);
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        return opts;
    }

    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, this.local);
    }

}
