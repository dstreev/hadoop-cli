package com.streever.hadoop.hdfs.shell.command;

import com.streever.hadoop.hdfs.util.FileSystemState;
import com.streever.hadoop.shell.Environment;
import org.apache.hadoop.fs.FileSystem;

public class PathBuilder {

    private Environment env;
    private PathDirectives directives;

    public PathBuilder(Environment env, PathDirectives directives) {
        this.env = env;
        this.directives = directives;
    }

    public PathBuilder(Environment env) {
        this.env = env;
        this.directives = new PathDirectives();
    }

    public String buildPath(Side side, String[] args) {
        String rtn = null;

//        FileSystem localfs = (FileSystem)env.getValue(Constants.LOCAL_FS);
//        FileSystem hdfs = (FileSystem) env.getValue(Constants.HDFS);
        FileSystemState lfss = env.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);
        FileSystemState fss = env.getFileSystemOrganizer().getCurrentFileSystemState();

        String in = null;

        switch (side) {
            case LEFT:
                if (args.length > 0)
                    if (directives.isBefore()) {
                        in = args[directives.getDirectives()];
                    } else {
                        if (directives.isOptional()) {
                            if (args.length > directives.getDirectives()) {
                                in = args[args.length-(directives.getDirectives()+1)];
                            } else {
                                // in is null
                            }
                        } else {
                            in = args[args.length-(directives.getDirectives()+1)];
                        }
                    }
                switch (directives.getDirection()) {
                    case REMOTE_LOCAL:
                    case REMOTE_REMOTE:
                    case NONE:
                        rtn = resolveFullPath(fss.getWorkingDirectory().toString(), in);
                        break;
                    case LOCAL_REMOTE:
                        rtn = in;
                        // this prefixes with remote, and we don't want that.  This is a local reference.
                        //resolveFullPath(fss.getWorkingDirectory().toString(), in);
                        break;
                }
                break;
            case RIGHT:
                if (args.length > 1)
                    if (directives.isBefore())
                        in = args[directives.getDirectives() + 1];
                    else
                        in = args[args.length-(directives.getDirectives()+1)];
                switch (directives.getDirection()) {
                    case REMOTE_LOCAL:
                        rtn = resolveFullPath(lfss.getWorkingDirectory().toString(), in);
                        break;
                    case LOCAL_REMOTE:
                    case REMOTE_REMOTE:
//                        rtn = resolveFullPath(hdfs.getWorkingDirectory().toString().substring(((String)env.getProperties().getProperty(Constants.HDFS_URL)).length()), in);
                        rtn = resolveFullPath(fss.getWorkingDirectory().toString(), in);
                        break;
                    case NONE:
                        break;
                }
                break;
        }
//        if (rtn != null && rtn.contains(" ")) {
//            rtn = "\"" + rtn + "\"";
//        }
        return rtn;
    }

    public static String resolveFullPath(String current, String input) {
        String adjusted = null;
        boolean enclose = false;
        if (input != null) {
            if (input.startsWith("\"") & input.endsWith("\"")) {
                adjusted = input.substring(1, input.length()-1);
            } else {
                adjusted = input;
            }
            if (!adjusted.startsWith("/") && !adjusted.startsWith("hdfs://") && !adjusted.startsWith("s3://") &&
                    !adjusted.startsWith("s3a://") && !adjusted.startsWith("gs://") && !adjusted.startsWith("adl://")
                    && !adjusted.startsWith("s3n://") && !adjusted.startsWith("wasb://") && !adjusted.startsWith("abfs://"))
                adjusted = current + "/" + adjusted;
        } else {
            adjusted = current;
//            return current;
        }
//        if (adjusted.contains(" ")) {
//            adjusted = "\\\"" + adjusted + "\\\"";
//        }
        return adjusted;
    }

}
