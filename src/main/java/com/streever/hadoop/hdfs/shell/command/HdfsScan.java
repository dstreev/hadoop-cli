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
import com.streever.hadoop.shell.command.CommandReturn;
import jline.console.completer.Completer;
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
    private Environment env;

    public HdfsScan(String name, Environment env) {
        super(name, env);
        this.env = env;
    }

    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {
        FileSystem hdfs = null;
        CommandReturn cr = commandReturn;
        try {
//            hdfs = (FileSystem) env.getValue(Constants.HDFS);
            FileSystemState fss = env.getFileSystemOrganizer().getCurrentFileSystemState();
            FileSystem fs = fss.getFileSystem();
            // Run lsp -f path for a list of paths in current directory.


            String dir = cmd.getArgs().length == 0 ? "/" : cmd.getArgs()[0];
            if (dir.startsWith("\"") & dir.endsWith("\"")) {
                dir = dir.substring(1, dir.length()-1);
            }
            logv(env, "CWD before: " + fss.getWorkingDirectory());
//            logv(env, "CWD before(env): " + env.getRemoteWorkingDirectory());
            logv(env, "Requested CWD: " + dir);


            Path newPath = null;
            if (dir.startsWith("/")) {
                newPath = new Path(fss.getURI(), dir);
            } else {
//                newPath = new Path(hdfs.getWorkingDirectory(), dir);
                newPath = new Path(fss.getWorkingDirectory(), dir);
            }

            Path qPath = newPath.makeQualified(hdfs);
            logv(env, "" + newPath);
            if (hdfs.getFileStatus(qPath).isDir() && hdfs.exists(qPath)) {
                hdfs.setWorkingDirectory(qPath);
            } else {
                log(env, "No such directory: " + dir);
            }

        } catch (IOException e) {
            cr.setCode(CODE_CMD_ERROR);
            cr.getErr().print(e.getMessage());
//            cr = new CommandReturn(CODE_CMD_ERROR, e.getMessage());
        } finally {
            FSUtil.prompt(env);
        }
        return cr;
    }

    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, false);
    }

}
