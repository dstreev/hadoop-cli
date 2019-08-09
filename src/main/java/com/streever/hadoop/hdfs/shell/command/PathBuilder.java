package com.streever.hadoop.hdfs.shell.command;

import com.streever.tools.stemshell.Environment;
import org.apache.hadoop.fs.FileSystem;

public class PathBuilder {

    public static String buildPath(Environment env, PathDirectives directives, String[] args) {
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

    public static String buildPath2(String current, String input) {
        if (input != null) {
            if (input.startsWith("/"))
                return input;
            else
                return current + "/" + input;
        } else {
            return current;
        }
    }

}
