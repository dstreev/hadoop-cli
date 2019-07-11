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

import java.io.IOException;

import com.streever.tools.stemshell.command.CommandReturn;
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

public class LocalMkdir extends HdfsCommand {

    public static final int LINE_COUNT = 10;

    private Environment env;
    private boolean local = false;

    public LocalMkdir(String name, Environment env, boolean local) {
        super(name, env);
//        this.env = env;
        this.local = local;
    }

    public CommandReturn execute(Environment env, CommandLine cmd, ConsoleReader console) {
        FileSystem hdfs = this.local ? (FileSystem) env.getValue(Constants.LOCAL_FS)
                        : (FileSystem) env.getValue(Constants.HDFS);
        logv(env, "CWD: " + hdfs.getWorkingDirectory());

        if (cmd.getArgs().length == 1) {
            Path path = new Path(hdfs.getWorkingDirectory(), cmd.getArgs()[0]);

            try {
                logv(env, "Create directory: " + path);
                hdfs.mkdirs(path);

            }
            catch (IOException e) {
                log(env, "Error creating directory '" + cmd.getArgs()[0]
                                + "': " + e.getMessage());
            }
        }
        else {
        }
        FSUtil.prompt(env);
        return CommandReturn.GOOD;
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        return opts;
    }

    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, this.local);
    }

}
