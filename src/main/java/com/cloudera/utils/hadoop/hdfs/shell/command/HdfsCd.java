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
import com.cloudera.utils.hadoop.shell.Environment;
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
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
        FileSystemNameCompleter fsc = new FileSystemNameCompleter(env);
        NullCompleter nullCompleter = new NullCompleter();
        Completer completer = new AggregateCompleter(fsc, nullCompleter);

        this.completer = completer;

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
