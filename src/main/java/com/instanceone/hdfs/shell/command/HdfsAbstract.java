// Copyright (c) 2012 P. Taylor Goetz (ptgoetz@gmail.com)

package com.instanceone.hdfs.shell.command;

import com.dstreev.hadoop.hdfs.shell.command.Constants;
import com.dstreev.hadoop.hdfs.shell.command.Direction;
import com.instanceone.hdfs.shell.completers.FileSystemNameCompleter;
import com.instanceone.stemshell.Environment;
import com.instanceone.stemshell.command.AbstractCommand;
import jline.console.completer.Completer;
import org.apache.hadoop.fs.FileSystem;

public abstract class HdfsAbstract extends AbstractCommand {
    protected Environment env;

    enum Side {
        LEFT,RIGHT
    }

    protected Direction directionContext = null;

    protected int directives = 0;
    protected boolean directivesBefore = true;
    protected boolean directivesOptional = false;

    public HdfsAbstract(String name) {
        super(name);
    }

    public HdfsAbstract(String name, Environment env, Direction directionContext ) {
        super(name);
        this.env = env;
        this.directionContext = directionContext;
    }

    public HdfsAbstract(String name, Environment env, Direction directionContext, int directives ) {
        super(name);
        this.env = env;
        this.directionContext = directionContext;
        this.directives = directives;
    }

    public HdfsAbstract(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional ) {
        super(name);
        this.env = env;
        this.directionContext = directionContext;
        this.directives = directives;
        this.directivesBefore = directivesBefore;
        this.directivesOptional = directivesOptional;
    }

    public HdfsAbstract(String name, Environment env) {
        super(name);
        this.env = env;
    }

    protected String buildPath(Side side, String[] args, Direction context) {
        String rtn = null;

        FileSystem localfs = (FileSystem)env.getValue(Constants.LOCAL_FS);
        FileSystem hdfs = (FileSystem) env.getValue(Constants.HDFS);

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
                        rtn = buildPath2(hdfs.getWorkingDirectory().toString().substring(((String)env.getProperty(Constants.HDFS_URL)).length()), in);
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
                        rtn = buildPath2(hdfs.getWorkingDirectory().toString().substring(((String)env.getProperty(Constants.HDFS_URL)).length()), in);
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

    protected String buildPath2(String current, String input) {
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
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, false);
    }


}
