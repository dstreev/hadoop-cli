/*
 * Copyright (c) 2022. David W. Streever All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.cloudera.utils.hadoop.hdfs.shell.command;

import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.cli.CliEnvironment;

public class PathBuilder {

    public enum SupportedProtocol {
        aws("s3a://","s3n://", "s3://"),
        google("gs://"),
        azure("wasb://","adl://","abfs://"),
        hadoop("hdfs://", "ofs://", "o3fs://"),
        current_dfs("/");
        private String[] protocols = null;

        SupportedProtocol(String... protocols) {
            this.protocols = protocols;
        }

        public String[] getProtocols() {
            return protocols;
        }


    }
    private CliEnvironment env;
    private PathDirectives directives;

    public PathBuilder(CliEnvironment env, PathDirectives directives) {
        this.env = env;
        this.directives = directives;
    }

    public PathBuilder(CliEnvironment env) {
        this.env = env;
        this.directives = new PathDirectives();
    }

    public static boolean isProtocolSupported(String path) {
        boolean rtn = Boolean.FALSE;
        for (SupportedProtocol supportedProtocol: SupportedProtocol.values()) {
            for (String protocol: supportedProtocol.getProtocols()) {
                if (path.startsWith(protocol)) {
                    rtn = Boolean.TRUE;
                    break;
                }
            }
            if (rtn)
                break;
        }
        return rtn;
    }

    public static boolean isPrefixWithKnownProtocol(String path) {
        boolean rtn = Boolean.FALSE;
        for (SupportedProtocol supportedProtocol: SupportedProtocol.values()) {
            switch (supportedProtocol) {
                case current_dfs:
                    // not prefixed.
                    break;
                default:
                    for (String protocol: supportedProtocol.getProtocols()) {
                        if (path.startsWith(protocol)) {
                            rtn = Boolean.TRUE;
                            break;
                        }
                    }
                    break;
            }
            if (rtn)
                break;
        }
        return rtn;
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
            if (!isProtocolSupported(adjusted)) {
                adjusted = current + "/" + adjusted;
            }
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
