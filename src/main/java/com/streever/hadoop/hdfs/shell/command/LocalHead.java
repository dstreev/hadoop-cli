package com.streever.hadoop.hdfs.shell.command;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import jline.console.ConsoleReader;
import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.streever.hadoop.hdfs.shell.completers.FileSystemNameCompleter;
import com.streever.tools.stemshell.Environment;

/**
 * Created by streever on 2015-11-22.
 */

public class LocalHead extends HdfsCommand {

    public static final int LINE_COUNT = 10;

    private Environment env;
    private boolean local = false;

    public LocalHead(String name, Environment env, boolean local) {
        super(name, env);
        this.env = env;
        this.local = local;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader console) {
        FileSystem hdfs = this.local ? (FileSystem) env.getValue(Constants.LOCAL_FS)
                        : (FileSystem) env.getValue(Constants.HDFS);
        logv(env, "CWD: " + hdfs.getWorkingDirectory());

        if (cmd.getArgs().length == 1) {
            int lineCount = Integer.parseInt(cmd.getOptionValue("n",
                            String.valueOf(LINE_COUNT)));
            Path path = new Path(hdfs.getWorkingDirectory(), cmd.getArgs()[0]);
            BufferedReader reader = null;
            try {
                InputStream is = hdfs.open(path);
                InputStreamReader isr = new InputStreamReader(is);
                reader = new BufferedReader(isr);
                String line = null;
                for (int i = 0; ((i <= lineCount) && (line = reader.readLine()) != null); i++) {
                    log(env, line);
                }
            }
            catch (IOException e) {
                log(env, "Error reading file '" + cmd.getArgs()[0]
                                + "': " + e.getMessage());
            }
            finally {
                try {
                    if (reader != null) {
                        reader.close();
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        else {
            // usage();
        }
        FSUtil.prompt(env);
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("n", "linecount", true,
                        "number of lines to display (defaults to 10)");
        return opts;
    }

    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, this.local);
    }

}
