// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import com.instanceone.stemshell.Environment;
import com.instanceone.stemshell.command.AbstractCommand;
import jline.console.ConsoleReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

public class HdfsCommand extends AbstractCommand {
    private Environment env;

    public static final String HDFS_URL = "hdfs.url";
    public static final String HDFS = "hdfs.fs";
    public static final String LOCAL_FS = "local.fs";
    public static final String CFG = "config";

    public enum Direction {
        LOCAL_REMOTE,
        REMOTE_LOCAL,
        REMOTE_REMOTE,
        NONE;
    }

    enum Side {
        LEFT,RIGHT;
    }
    private Direction directionContext = null;


    public HdfsCommand(String name) {
        super(name);
    }

    public HdfsCommand(String name, Environment env, Direction directionContext ) {
        super(name);
        this.env = env;
        this.directionContext = directionContext;
    }

    public HdfsCommand(String name, Environment env) {
        super(name);
        this.env = env;
    }

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        FsShell shell = new FsShell();

        Configuration conf = (Configuration)env.getValue(HdfsCommand.CFG);

        String hdfs_uri = (String)env.getProperty(HdfsCommand.HDFS_URL);

        FileSystem hdfs = (FileSystem) env.getValue(HDFS);

        if (hdfs == null) {
            System.out.println("Please connect first");
        }
        conf.set("fs.defaultFS", hdfs_uri);

        conf.setQuietMode(false);
        shell.setConf(conf);
        int res;
        String[] argv = null;

        Option[] cmdOpts = cmd.getOptions();
        String[] cmdArgs = cmd.getArgs();

        // TODO:  Need to Handle context aware file operations.
        // put, get, mv, copy.., chmod, chown, chgrp, count
        int fieldPlus = 0;

        String leftPath = null;
        String rightPath = null;

        switch (directionContext) {
            case REMOTE_LOCAL:
                fieldPlus += 2; // Source and Destination Path Elements.
                break;
            case LOCAL_REMOTE:
                fieldPlus += 2; // Source and Destination Path Elements.

                break;
            case REMOTE_REMOTE:
                fieldPlus += 2; // Source and Destination Path Elements.

                break;
            default: // NONE
                fieldPlus += 1;
        }

        leftPath = buildPath(Side.LEFT, cmdArgs, directionContext);
        rightPath = buildPath(Side.RIGHT, cmdArgs, directionContext);

        if (rightPath != null) {
            cmdArgs = new String[2];
            cmdArgs[0] = leftPath;
            cmdArgs[1] = rightPath;
        } else {
            cmdArgs = new String[1];
            cmdArgs[0] = leftPath;
        }

        argv = new String[cmdOpts.length + cmdArgs.length + 1];

        int pos = 1;

        for (Option opt: cmdOpts) {
            argv[pos++] = "-" + opt.getOpt();
        }

        for (String arg: cmdArgs) {
            argv[pos++] = arg;
        }

        argv[0] = "-" + getName();

        System.out.println("HDFS Command: " + Arrays.toString(argv));

        try {
            res = ToolRunner.run(shell, argv);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                shell.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String buildPath(Side side, String[] args, Direction context) {
        String rtn = null;

        FileSystem localfs = (FileSystem)env.getValue(LOCAL_FS);
        FileSystem hdfs = (FileSystem) env.getValue(HDFS);

        String in = null;

        switch (side) {
            case LEFT:
                if (args.length > 0)
                    in = args[0];
                switch (context) {
                    case REMOTE_LOCAL:
                    case REMOTE_REMOTE:
                    case NONE:
                        rtn = buildPath2(hdfs.getWorkingDirectory().toString().substring(((String)env.getProperty(HdfsCommand.HDFS_URL)).length()), in);
                        break;
                    case LOCAL_REMOTE:
                        rtn = buildPath2(localfs.getWorkingDirectory().toString().substring(5), in);
                        break;
                }
                break;
            case RIGHT:
                if (args.length > 1)
                    in = args[1];
                switch (context) {
                    case REMOTE_LOCAL:
                        rtn = buildPath2(localfs.getWorkingDirectory().toString().substring(5), in);
                        break;
                    case LOCAL_REMOTE:
                    case REMOTE_REMOTE:
                        rtn = buildPath2(hdfs.getWorkingDirectory().toString().substring(((String)env.getProperty(HdfsCommand.HDFS_URL)).length()), in);
                        break;
                    case NONE:
                        break;
                }
                break;
        }
        return rtn;
    }

    private String buildPath2(String current, String input) {
        if (input != null) {
            if (input.startsWith("/"))
                return input;
            else
                return current + "/" + input;
        } else {
            return current;
        }
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("l", false, "show extended file attributes");
        opts.addOption("R", false, "recurse");
        opts.addOption("f", false, "force");
        opts.addOption("p", false, "preserve");
        opts.addOption("h", false, "human readable");
        opts.addOption("s", false, "summary");
        opts.addOption("d", false, "dump");
        opts.addOption("e", false, "encoding");
        opts.addOption("t", false, "sort by Timestamp");
        opts.addOption("S", false, "sort by Size");
        opts.addOption("r", false, "reverse");
        opts.addOption("u", false, "user access time");
        opts.addOption("skipTrash", false, "Skip Trash");
//        opts.addOption("ignore-fail-on-non-empty", false, "ignore-fail-on-non-empty");
        return opts;
    }

}
