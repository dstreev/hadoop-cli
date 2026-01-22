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
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by streever on 2015-11-22.
 */

public class LocalMkdir extends HdfsCommand {

    public static final int LINE_COUNT = 10;

    public LocalMkdir(String name) {
        super(name);
    }

    public CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn) {
        FileSystemState lfss = session.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);
        FileSystem lfs = lfss.getFileSystem();

        AbstractCommand.logv(session, "CWD: " + lfss.getWorkingDirectory());

        if (cmd.getArgs().length == 1) {
            Path path = new Path(lfss.getWorkingDirectory(), cmd.getArgs()[0]);

            try {
                AbstractCommand.logv(session, "Create directory: " + path);
                lfs.mkdirs(path);

            }
            catch (IOException e) {
                AbstractCommand.log(session, "Error creating directory '" + cmd.getArgs()[0]
                                + "': " + e.getMessage());
            }
        }
        else {
        }
        return commandReturn;
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
        return opts;
    }


}
