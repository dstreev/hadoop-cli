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
import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
/*
This will wrap other functions by looping through the target directory and applying the
function to each directory.

For example: Current directory is /user/dstreev.  Which has 4 sub-directories: data, temp, working, and checking

scan ls

would do an 'ls' operation on each of the sub-directories.

 */
public class HdfsScan extends HdfsCommand {

    public HdfsScan(String name) {
        super(name);
    }

    @Override
    public Completer getCompleter() {
        return new NullCompleter();
    }

    public CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn) {
        FileSystem hdfs = null;
        CommandReturn cr = commandReturn;
        try {
            FileSystemState fss = session.getFileSystemOrganizer().getCurrentFileSystemState();
            FileSystem fs = fss.getFileSystem();
            // Run lsp -f path for a list of paths in current directory.


            String dir = cmd.getArgs().length == 0 ? "/" : cmd.getArgs()[0];
            if (dir.startsWith("\"") & dir.endsWith("\"")) {
                dir = dir.substring(1, dir.length()-1);
            }
            logv(session, "CWD before: " + fss.getWorkingDirectory());
            logv(session, "Requested CWD: " + dir);


            Path newPath = null;
            if (dir.startsWith("/")) {
                newPath = new Path(fss.getURI(), dir);
            } else {
                newPath = new Path(fss.getWorkingDirectory(), dir);
            }

            Path qPath = newPath.makeQualified(hdfs);
            logv(session, "" + newPath);
            if (hdfs.getFileStatus(qPath).isDir() && hdfs.exists(qPath)) {
                hdfs.setWorkingDirectory(qPath);
            } else {
                log(session, "No such directory: " + dir);
            }

        } catch (IOException e) {
            cr.setCode(CODE_CMD_ERROR);
            cr.getErr().print(e.getMessage());
        } finally {
        }
        return cr;
    }

}
