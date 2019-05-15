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

import com.streever.tools.stemshell.Environment;
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

public class HdfsCommand extends HdfsAbstract {

    public HdfsCommand(String name) {
        super(name);
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

    public void execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        FsShell shell = new FsShell();

        Configuration conf = (Configuration)env.getValue(Constants.CFG);

        String hdfs_uri = (String)env.getProperty(Constants.HDFS_URL);

        FileSystem hdfs = (FileSystem) env.getValue(Constants.HDFS);

        if (hdfs == null) {
            loge(env, "Please connect first");
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

        logv(env, "HDFS Command: " + Arrays.toString(argv));

        try {
            res = ToolRunner.run(shell, argv);
            if (res != 0) {
                StringBuilder sb = new StringBuilder("ERROR");
                for (String arg: argv) {
                    sb.append("\t");
                    sb.append(arg);
                }
                this.loge(env, sb.toString());
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

//    @Override
//    public Completer getCompleter() {
//        return new FileSystemNameCompleter(this.env, false);
//    }


}
