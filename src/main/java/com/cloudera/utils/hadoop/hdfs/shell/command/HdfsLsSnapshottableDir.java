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

import com.cloudera.utils.hadoop.cli.CliSession;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemOrganizer;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;

import java.io.IOException;

@Slf4j
public class HdfsLsSnapshottableDir extends HdfsAbstract {

    public HdfsLsSnapshottableDir(String name) {
        this(name, Direction.NONE);
    }

    @Override
    public String getDescription() {
        return "List Snapshottable HDFS Directories";
    }

    public HdfsLsSnapshottableDir(String name, Direction directionContext) {
        super(name, directionContext);
    }

    public HdfsLsSnapshottableDir(String name, Direction directionContext, int directives) {
        super(name, directionContext, directives);
    }

    public HdfsLsSnapshottableDir(String name, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, directionContext, directives, directivesBefore, directivesOptional);
    }

    @Override
    public Completer getCompleter() {
        return new NullCompleter();
    }

    @Override
    public CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn) {
        CommandReturn cr = commandReturn;
        FileSystemOrganizer fso = session.getFileSystemOrganizer();
        try {
            // Check connect protocol
            if (fso.isCurrentDefault()) {

                FileSystemState fss = fso.getCurrentFileSystemState();

                DistributedFileSystem dfs = (DistributedFileSystem) fss.getFileSystem();

                if (dfs == null) {
                    cr.setCode(AbstractCommand.CODE_NOT_CONNECTED);
                    cr.setError(("Connect first"));
                    err.println("Connect first");
                    return cr;
                }

                // TODO: add coloring to output
                SnapshottableDirectoryStatus[] stats = dfs.getSnapshottableDirListing();
                SnapshottableDirectoryStatus.print(stats, cr.getOut());

            } else {
                log.error("This function is only available for the 'default' namespace");
                cr.setCode(-1);
                cr.setError("Not available for alternate namespace: " +
                        fso.getCurrentFileSystemState().getNamespace());
                return cr;
            }

        } catch (RuntimeException | IOException rt) {
            log.error("Issue with command: {}", cmd.toString(), rt);
        }
        return cr;
    }

}
