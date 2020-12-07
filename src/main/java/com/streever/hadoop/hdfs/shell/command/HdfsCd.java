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
import com.streever.hadoop.shell.Environment;
import com.streever.hadoop.shell.command.AbstractCommand;
import com.streever.hadoop.shell.command.CommandReturn;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
import jline.console.completer.StringsCompleter;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsCd extends AbstractCommand {
    private Environment env;

    public HdfsCd(String name, Environment env) {
        super(name);
        this.env = env;
        // Completer
//        StringsCompleter sc = new StringsCompleter(name);
        FileSystemNameCompleter fsc = new FileSystemNameCompleter(env, false);
        NullCompleter nc = new NullCompleter();
        this.completer = new AggregateCompleter(fsc, nc);

    }

    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn cr) {
        try {
            if (env.getFileSystemOrganizer().isCurrentLocal()) {
                FileSystemState lfss = env.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);
                FileSystem localfs = env.getFileSystemOrganizer().getLocalFileSystem();//(FileSystem) env.getValue(Constants.LOCAL_FS);
                String dir = cmd.getArgs().length == 0 ? System
                        .getProperty("user.home") : cmd.getArgs()[0];
                logv(env, "Change Dir to: " + dir);
                logv(env, "CWD: " + lfss.getWorkingDirectory());

                Path newPath = null;

                if (dir.startsWith("~/")) {
                    dir = lfss.getHomeDir(env) + (dir.substring(1).length() > 1 ? dir.substring(1) : "");
                    newPath = new Path(dir);
                } else if (dir.startsWith("/")) {
                    newPath = new Path(dir);
                } else {
                    newPath = new Path(lfss.getWorkingDirectory(), dir);
                }

                FileStatus fstat = lfss.getFileSystem().getFileStatus(newPath);
                if (localfs.exists(newPath)) {
                    logv(env, "exists");
                    if (fstat.isDirectory()) {
                        lfss.setWorkingDirectory(newPath);
                    } else {
                        logv(env, "Is not a directory: " + dir);
                    }
                }

//                FSUtil.prompt(env);

            } else {
                FileSystemState fss = env.getFileSystemOrganizer().getCurrentFileSystemState();
                FileSystem fs = fss.getFileSystem();

                String dir = cmd.getArgs().length == 0 ? "/" : cmd.getArgs()[0];
                if (dir.startsWith("\"") & dir.endsWith("\"")) {
                    dir = dir.substring(1, dir.length() - 1);
                }

                Path newPath = null;
                Path newWorking = null;
                if (dir.startsWith("~")) {
                    dir = fss.getHomeDir(env) + (dir.substring(1).length() > 1 ? dir.substring(1) : "");
                    newPath = new Path(fss.getURI(), dir);
                } else if (dir.startsWith("/")) {
                    newPath = new Path(fss.getURI(), dir);
                } else {
                    newWorking = new Path(fss.getWorkingDirectory(), dir);
                    newPath = new Path(fss.getURI(), newWorking);
                }

                logv(env, "" + newPath);
                if (fss.equals(env.getFileSystemOrganizer().getDefaultFileSystemState())) {
                    FileStatus fstat = fss.getFileSystem().getFileStatus(newPath);
                    if (fs.exists(newPath)) {
                        logv(env, "exists");
                        if (fstat.isDirectory()) {
                            fss.setWorkingDirectory(newPath);
                        } else {
                            logv(env, "Is not a directory: " + dir);
                        }
                    }
                } else {
                    // Can't get stats from alt namespaces.
                    fss.setWorkingDirectory(newPath);
                }
            }
        } catch (Throwable throwable) {
            cr.setCode(CODE_CMD_ERROR);
            cr.getErr().print(throwable.getMessage());
            throwable.printStackTrace();
        } finally {
//            FSUtil.prompt(env);
        }
        return cr;
    }

    @Override
    public String getDescription() {
        return "Remote Change Directory";
    }

//    @Override
//    public Completer getCompleter() {
//        return new FileSystemNameCompleter(this.env, false);
//    }

}
