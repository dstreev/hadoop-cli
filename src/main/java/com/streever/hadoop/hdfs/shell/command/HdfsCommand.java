/*
 *  Hadoop CLI
 *
 *  (c) 2016-2019 David W. Streever. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with David W. Streever, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with David W. Streever or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) David W. Streever PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) David W. Streever DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) David W. Streever IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *    FROM OR RELATED TO THE CODE; AND
 *  (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, David W. Streever IS NOT LIABLE FOR ANY
 *    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 *
 */
package com.streever.hadoop.hdfs.shell.command;

import com.streever.hadoop.hdfs.shell.completers.FileSystemNameCompleter;
import com.streever.hadoop.hdfs.util.FileSystemState;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
import jline.console.completer.StringsCompleter;
import org.apache.hadoop.fs.CliFsShell;
import com.streever.hadoop.shell.Environment;
import com.streever.hadoop.shell.command.CommandReturn;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
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
        // Completer

//        FileSystemNameCompleter fsc = new FileSystemNameCompleter(env);
//        NullCompleter nullCompleter = new NullCompleter();
//        Completer completer = new AggregateCompleter(fsc, nullCompleter);
//
//        this.completer = completer;

    }

    public HdfsCommand(String name, Environment env, Direction directionContext, int directives ) {
        super(name,env,directionContext,directives);
    }

    public HdfsCommand(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional ) {
        super(name,env,directionContext,directives,directivesBefore,directivesOptional);
    }

    public HdfsCommand(String name, Environment env) {
        this(name,env, Direction.NONE);
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
            leftPath = fss.getURI() + leftPath;
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

        for (Option opt: cmdOpts) {
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

        for (String arg: newCmdArgs) {
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
