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

import java.io.IOException;

import com.cloudera.utils.hadoop.cli.CliSession;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LocalCd extends HdfsCommand {

    public LocalCd(String name) {
        super(name);
    }

    public CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn) {
        try {

            FileSystemState lfss = session.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);
            FileSystem localfs = session.getFileSystemOrganizer().getLocalFileSystem();
            String dir = cmd.getArgs().length == 0 ? System
                            .getProperty("user.home") : cmd.getArgs()[0];
            AbstractCommand.logv(session, "Change Dir to: " + dir);
            AbstractCommand.logv(session, "CWD: " + lfss.getWorkingDirectory());

            Path newPath = null;

            if (dir.startsWith("~")) {
                dir = System.getProperty("user.home") + (dir.substring(1).length() > 1?dir.substring(1):"");
                newPath = new Path(dir);
            } else if (dir.startsWith("/")) {
                newPath = new Path(dir);
            } else {
                newPath = new Path(lfss.getWorkingDirectory(), dir);
            }

            FileStatus fstat = lfss.getFileSystem().getFileStatus(newPath);
            if (localfs.exists(newPath)) {
                AbstractCommand.logv(session, "exists");
                if (fstat.isDirectory()) {
                    lfss.setWorkingDirectory(newPath);
                } else {
                    AbstractCommand.logv(session, "Is not a directory: " + dir);
                }
            }

        }
        catch (IOException e) {
            AbstractCommand.log(session, e.getMessage());
            commandReturn.setCode(AbstractCommand.CODE_LOCAL_FS_ISSUE);
            commandReturn.getErr().print(e.getMessage());
        }
        return commandReturn;
    }


}
