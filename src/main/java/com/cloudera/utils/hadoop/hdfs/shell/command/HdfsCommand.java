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

import com.cloudera.utils.hadoop.hdfs.shell.completers.FileSystemNameCompleter;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
import org.apache.hadoop.fs.CliFsShell;
import com.cloudera.utils.hadoop.shell.Environment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

public class HdfsCommand extends HdfsAbstract {

    public HdfsCommand(String name) {
        this(name, null, Direction.NONE);
    }

    @Override
    public String getDescription() {
        return "Native `hdfs` command";
    }

    public HdfsCommand(String name, Environment env, Direction directionContext ) {
        super(name, env, directionContext);
    }

    public HdfsCommand(String name, Environment env, Direction directionContext, int directives ) {
        super(name,env,directionContext,directives);
    }

    public HdfsCommand(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional ) {
        super(name,env,directionContext,directives,directivesBefore,directivesOptional);
    }

    public HdfsCommand(String name, Environment env) {
        super(name,env);
    }

    @Override
    public Completer getCompleter() {
        FileSystemNameCompleter fsc = new FileSystemNameCompleter(env);
        NullCompleter nullCompleter = new NullCompleter();
        Completer completer = new AggregateCompleter(fsc, nullCompleter);

        return completer;
    }

    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn cr) {
        Configuration conf = (Configuration)env.getConfig();
        CliFsShell shell = new CliFsShell(conf);
        try {
            shell.init();
        } catch (IOException e) {
            e.printStackTrace();
        }
        shell.setOut(cr.getOut());
        shell.setErr(cr.getErr());

        FileSystemState fss = env.getFileSystemOrganizer().getCurrentFileSystemState();

        conf.setQuietMode(false);
        shell.setConf(conf);
        int res = CODE_SUCCESS;
        String[] argv = null;

        Option[] cmdOpts = cmd.getOptions();
        String[] cmdArgs = cmd.getArgs();

        // TODO:  Need to Handle context aware file operations.
        // put, get, mv, copy.., chmod, chown, chgrp, count
        int pathCount = 0;

        String leftPath = null;
        String rightPath = null;

        if (pathDirectives != null) {
            switch (pathDirectives.getDirection()) {
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

            leftPath = pathBuilder.buildPath(Side.LEFT, cmdArgs);

            // When the fs isn't the 'default', we need to 'fully' qualify it.
            // When dealing with non-default/non-local filesystems, we need to prefix the uri with the namespace.
            // For LOCAL_REMOTE calls, like put, don't prefix the leftPath.
            if (!fss.equals(env.getFileSystemOrganizer().getDefaultFileSystemState()) && pathDirectives.getDirection() != Direction.LOCAL_REMOTE) {
                // If leftPath starts with a known fs protocol, don't modify.
                if (!pathBuilder.isPrefixWithKnownProtocol(leftPath)) {
                    leftPath = fss.getURI() + leftPath;
                }
            }

            if (pathDirectives.getDirection() != Direction.NONE) {
                rightPath = pathBuilder.buildPath(Side.RIGHT, cmdArgs);
                // When dealing with non-default/non-local filesystems, we need to prefix the uri with the namespace.
                if (!fss.equals(env.getFileSystemOrganizer().getDefaultFileSystemState())) {
                    rightPath = fss.getURI() + rightPath;
                }
            }

            String[] newCmdArgs = new String[pathCount];
            if (rightPath != null) {
                newCmdArgs[0] = leftPath;
                newCmdArgs[1] = rightPath;
            } else {
                newCmdArgs[0] = leftPath;
            }

            argv = new String[cmdOpts.length + newCmdArgs.length + 1 + pathDirectives.getDirectives()];

            int pos = 1;

            for (Option opt : cmdOpts) {
                if (pos >= argv.length) {
                    System.out.println("OUT OF BOUNDS: " + pos + " " + argv.length);
                }
                argv[pos++] = "-" + opt.getOpt();
            }

            if (pathDirectives.isBefore()) {
                for (int i = 0; i < pathDirectives.getDirectives(); i++) {
                    argv[pos++] = cmdArgs[i];
                }
            }

            for (String arg : newCmdArgs) {
                argv[pos++] = arg;
            }

            if (!pathDirectives.isBefore()) {
                for (int i = pathDirectives.getDirectives(); i > 0; i--) {
                    try {
                        argv[pos++] = cmdArgs[cmdArgs.length - (i)];
                    } catch (Exception e) {
                        // Can happen when args are optional
                    }
                }
            }
        } else {
            if (cmdArgs.length == 0) {
                argv = new String[2];
            } else {
                argv = new String[cmdArgs.length + 1];
            }
            // Assume first parameter is a path element.
            String path = pathBuilder.buildPath(Side.LEFT, cmdArgs);
            argv[1] = path;
            if (cmdArgs.length > 1) {
                for (int i = 2; i <= argv.length - 1; i++) {
                    argv[i] = cmdArgs[i-1];
                }
            }
        }

        argv[0] = "-" + getName();

        logv(env, "HDFS Command: " + Arrays.toString(argv));

        try {
            cr.setCommandArgs(argv);
            // TODO: Test for right PATH
            cr.setPath(leftPath);
            res = ToolRunner.run(shell, argv);
            if (res != 0) {
//                StringBuilder sb = new StringBuilder();
//                for (String arg: argv) {
//                    sb.append("\t");
//                    sb.append(arg);
//                }
                cr.setCode(res);
//                cr.getErr().print(sb.toString());
//                cr.setDetails(sb.toString());//
                //  cr = new CommandReturn(res, sb.toString());
//                this.loge(env, sb.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                shell.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return cr;
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

//        opts.addOption("c", true, "Comment");
//        opts.addOption("ignore-fail-on-non-empty", false, "ignore-fail-on-non-empty");
        return opts;
    }

    @Override
    protected void processCommandLine(CommandLine commandLine) {
       super.processCommandLine(commandLine);
    }
    

}
