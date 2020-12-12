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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.streever.hadoop.hdfs.util.FileSystemState;
import com.streever.hadoop.shell.command.CommandReturn;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.Completer;

import jline.console.completer.NullCompleter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.streever.hadoop.hdfs.shell.completers.FileSystemNameCompleter;
import com.streever.hadoop.shell.Environment;

/**
 * Created by streever on 2015-11-22.
 */

public class LocalHead extends HdfsCommand {

    public static final int LINE_COUNT = 10;

//    private Environment env;
//    private boolean local = false;

    public LocalHead(String name, Environment env) {
        super(name, env);

        FileSystemNameCompleter fsc = new FileSystemNameCompleter(env, true);
        NullCompleter nullCompleter = new NullCompleter();
        Completer completer = new AggregateCompleter(fsc, nullCompleter);

        this.completer = completer;
        
    }

    @Override
    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {
        CommandReturn cr = commandReturn;
//        int rtn = CODE_SUCCESS;
//        FileSystem hdfs = this.local ? (FileSystem) env.getValue(Constants.LOCAL_FS)
//                        : (FileSystem) env.getValue(Constants.HDFS);
        FileSystemState lfss = env.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);
        FileSystem lfs = lfss.getFileSystem();

        logv(env, "CWD: " + lfss.getWorkingDirectory());
//        logv(env, "CWD(env): " + env.getRemoteWorkingDirectory());

        if (cmd.getArgs().length == 1) {
            int lineCount = Integer.parseInt(cmd.getOptionValue("n",
                            String.valueOf(LINE_COUNT)));
//            Path path = new Path(hdfs.getWorkingDirectory(), cmd.getArgs()[0]);
            Path path = new Path(lfss.getWorkingDirectory(), cmd.getArgs()[0]);
            BufferedReader reader = null;
            try {
                InputStream is = lfs.open(path);
                InputStreamReader isr = new InputStreamReader(is);
                reader = new BufferedReader(isr);
                String line = null;
                for (int i = 0; ((i <= lineCount) && (line = reader.readLine()) != null); i++) {
                    log(env, line);
                }
            }
            catch (IOException e) {
//                log(env, "Error reading file '" + cmd.getArgs()[0]
//                                + "': " + e.getMessage());
                cr.setCode(CODE_CMD_ERROR);
                cr.getErr().print("Error reading file '" + cmd.getArgs()[0]
                                + "': " + e.getMessage());
            }
            finally {
                try {
                    if (reader != null) {
                        reader.close();
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        else {
            // usage();
        }
//        FSUtil.prompt(env);
        return cr;
    }

    @Override
    public String getDescription() {
        return "Local `head` command";
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        opts.addOption("n", "linecount", true,
                        "number of lines to display (defaults to 10)");
        return opts;
    }

//    @Override
//    public Completer getCompleter() {
//        return new FileSystemNameCompleter(this.env, this.local);
//    }

}
