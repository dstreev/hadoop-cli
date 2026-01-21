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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.cloudera.utils.hadoop.cli.CliSession;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by streever on 2015-11-22.
 */
@Slf4j
public class LocalHead extends HdfsCommand {

    public static final int LINE_COUNT = 10;

    public LocalHead(String name) {
        super(name);
    }

    @Override
    public CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn) {
        CommandReturn cr = commandReturn;

        FileSystemState lfss = session.getFileSystemOrganizer().getFileSystemState(Constants.LOCAL_FS);
        FileSystem lfs = lfss.getFileSystem();

        logv(session, "CWD: " + lfss.getWorkingDirectory());

        if (cmd.getArgs().length == 1) {
            int lineCount = Integer.parseInt(cmd.getOptionValue("n",
                            String.valueOf(LINE_COUNT)));
            Path path = new Path(lfss.getWorkingDirectory(), cmd.getArgs()[0]);
            BufferedReader reader = null;
            try {
                InputStream is = lfs.open(path);
                InputStreamReader isr = new InputStreamReader(is);
                reader = new BufferedReader(isr);
                String line = null;
                for (int i = 0; ((i <= lineCount) && (line = reader.readLine()) != null); i++) {
                    log(session, line);
                }
            }
            catch (IOException e) {
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
                    log.error("Error closing reader: {} - {}", e.getMessage(), e.getCause(), e);
                }
            }
        }
        else {
        }
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
}
