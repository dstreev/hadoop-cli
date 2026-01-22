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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.SnapshotDiffReport;

public class SnapshotDiff extends HdfsAbstract {

    /**
     * Construct a SnapshotDiff object.
     */
    public SnapshotDiff(String name) {
        super(name);
    }

    @Override
    public Completer getCompleter() {
        return new NullCompleter();
    }

    private static String getSnapshotName(String name) {
        if (Path.CUR_DIR.equals(name)) { // current directory
            return "";
        }
        final int i;
        if (name.startsWith(HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR)) {
            i = 0;
        } else if (name.startsWith(
                HdfsConstants.SEPARATOR_DOT_SNAPSHOT_DIR + Path.SEPARATOR)) {
            i = 1;
        } else {
            return name;
        }

        // get the snapshot name
        return name.substring(i + HdfsConstants.DOT_SNAPSHOT_DIR.length() + 1);
    }

    @Override
    public CommandReturn implementation(CliSession session, CommandLine cmdr, CommandReturn cr) {
        String fromSnapshot, toSnapshot = null;
        Path snapshotRoot = null;

        try {
            if (session.getFileSystemOrganizer().isCurrentLocal()) {
                // Can't do this on local file system.
                cr.setCode(CODE_CMD_ERROR);
                cr.setError("SnapshotDiff can only be used in DistributedFileSystem");
            } else {
                FileSystemState fss = session.getFileSystemOrganizer().getCurrentFileSystemState();
                DistributedFileSystem dfs = (DistributedFileSystem) fss.getFileSystem();

                if (cmdr.getArgs().length == 2) {
                    /*
                    then its just the snapshot names and the path is current
                            OR
                    (wip) the first is a path item and the second is the snapshot.  We need to 'create' another
                    snapshot for the 'current' state and compare it with the supplied snapshot.
                    */
                    fromSnapshot = getSnapshotName(cmdr.getArgs()[0]);
                    toSnapshot = getSnapshotName(cmdr.getArgs()[1]);
                    snapshotRoot = fss.getWorkingDirectory();

                } else if (cmdr.getArgs().length == 3) {
                    //  parameters = 3, first item is the path, next are the snapshots.
                    String dir = cmdr.getArgs()[0];

                    fromSnapshot = getSnapshotName(cmdr.getArgs()[1]);
                    toSnapshot = getSnapshotName(cmdr.getArgs()[2]);

                    if (dir.startsWith("\"") & dir.endsWith("\"")) {
                        dir = dir.substring(1, dir.length() - 1);
                    }

                    Path newWorking = null;
                    if (dir.startsWith("~")) {
                        dir = fss.getHomeDir(session) + (dir.substring(1).length() > 1 ? dir.substring(1) : "");
                        snapshotRoot = new Path(fss.getURI(), dir);
                    } else if (dir.startsWith("/")) {
                        snapshotRoot = new Path(fss.getURI(), dir);
                    } else {
                        newWorking = new Path(fss.getWorkingDirectory(), dir);
                        snapshotRoot = new Path(fss.getURI(), newWorking);
                    }

                } else {
                    cr.setCode(CODE_CMD_ERROR);
                    cr.setError("Requires either 2 or 3 parameters");
                    return cr;
                }

                SnapshotDiffReport diffReport = dfs.getSnapshotDiffReport(snapshotRoot,
                        fromSnapshot, toSnapshot);
                cr.getOut().println(diffReport.toString());

            }
        } catch (Throwable throwable) {
            cr.setCode(CODE_CMD_ERROR);
            cr.getErr().print(throwable.getMessage());
            throwable.printStackTrace();
        } finally {
        }
        return cr;


    }

    @Override
    public String getDescription() {
        String description = "hdfs snapshotDiff <snapshotDir> <from> <to>:\n" +
                "\tGet the difference between two snapshots, \n" +
                "\tor between a snapshot and the current tree of a directory.\n" +
                "\tFor <from>/<to>, users can use \".\" to present the current status,\n" +
                "\tand use \".snapshot/snapshot_name\" to present a snapshot,\n" +
                "\twhere \".snapshot/\" can be omitted\n";
        return description;
    }

}
