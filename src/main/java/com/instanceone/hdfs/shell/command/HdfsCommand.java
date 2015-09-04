// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import com.instanceone.hdfs.shell.completers.FileSystemNameCompleter;
import com.instanceone.stemshell.Environment;
import com.instanceone.stemshell.command.AbstractCommand;
import jline.console.ConsoleReader;
import jline.console.completer.Completer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.security.UserGroupInformation;
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

    private int directives = 0;
    private boolean directivesBefore = true;
    private boolean directivesOptional = false;

    public HdfsCommand(String name) {
        super(name);
    }

    public HdfsCommand(String name, Environment env, Direction directionContext ) {
        super(name);
        this.env = env;
        this.directionContext = directionContext;
    }

    public HdfsCommand(String name, Environment env, Direction directionContext, int directives ) {
        super(name);
        this.env = env;
        this.directionContext = directionContext;
        this.directives = directives;
    }

    public HdfsCommand(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional ) {
        super(name);
        this.env = env;
        this.directionContext = directionContext;
        this.directives = directives;
        this.directivesBefore = directivesBefore;
        this.directivesOptional = directivesOptional;
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
        int pathCount = 0;

        String leftPath = null;
        String rightPath = null;

        switch (directionContext) {
            case REMOTE_LOCAL:
                pathCount += 2; // Source and Destination Path Elements.
                break;
            case LOCAL_REMOTE:
                pathCount += 2; // Source and Destination Path Elements.

                break;
            case REMOTE_REMOTE:
                pathCount += 2; // Source and Destination Path Elements.

                break;
            default: // NONE
                pathCount += 1;
        }

        leftPath = buildPath(Side.LEFT, cmdArgs, directionContext);
        if (directionContext != Direction.NONE) {
            rightPath = buildPath(Side.RIGHT, cmdArgs, directionContext);
        }

        String[] newCmdArgs = new String[pathCount];
        if (rightPath != null) {
            newCmdArgs[0] = leftPath;
            newCmdArgs[1] = rightPath;
        } else {
            newCmdArgs[0] = leftPath;
        }

        argv = new String[cmdOpts.length + newCmdArgs.length + 1 + directives];

        int pos = 1;

        for (Option opt: cmdOpts) {
            argv[pos++] = "-" + opt.getOpt();
        }

        if (directivesBefore) {
            for (int i = 0; i < directives; i++) {
                argv[pos++] = cmdArgs[i];
            }
        }

        for (String arg: newCmdArgs) {
            argv[pos++] = arg;
        }

        if (!directivesBefore) {
            for (int i = directives; i > 0; i--) {
                try {
                    argv[pos++] = cmdArgs[cmdArgs.length - (i)];
                } catch (Exception e) {
                    // Can happen when args are optional
                }
            }
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
                    if (directivesBefore) {
                        in = args[directives];
                    } else {
                        if (directivesOptional) {
                            if (args.length > directives) {
                                in = args[args.length-(directives+1)];
                            } else {
                                // in is null
                            }
                        } else {
                            in = args[args.length-(directives+1)];
                        }
                    }
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
                    if (directivesBefore)
                        in = args[directives + 1];
                    else
                        in = args[args.length-(directives+1)];
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
        if (rtn != null && rtn.contains(" ")) {
            rtn = "'" + rtn + "'";
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
        opts.addOption("f", false, "force / is file");
        opts.addOption("p", false, "preserve");
        opts.addOption("h", false, "human readable");
        opts.addOption("s", false, "summary");
        opts.addOption("q", false, "query");
        opts.addOption("d", false, "dump / path is directory");
        opts.addOption("e", false, "encoding / path exists");
        opts.addOption("t", false, "sort by Timestamp");
        opts.addOption("S", false, "sort by Size / path not empty");
        opts.addOption("r", false, "reverse");
        opts.addOption("z", false, "file length is 0");
        opts.addOption("u", false, "user access time");
        opts.addOption("skipTrash", false, "Skip Trash");
        opts.addOption("ignorecrc", false, "ignorecrc");
        opts.addOption("crc", false, "crc");

//        opts.addOption("ignore-fail-on-non-empty", false, "ignore-fail-on-non-empty");
        return opts;
    }

    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, false);
    }


}
