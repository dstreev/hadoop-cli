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

import java.io.IOException;

import com.streever.hadoop.shell.command.AbstractCommand;
import com.streever.hadoop.shell.command.CommandReturn;
import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.streever.hadoop.hdfs.shell.completers.FileSystemNameCompleter;
import com.streever.hadoop.shell.Environment;

public class HdfsCd extends AbstractCommand {
    private Environment env;

    public HdfsCd(String name, Environment env) {
        super(name);
        this.env = env;
    }

    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn cr) {
        FileSystem hdfs = null;
//        CommandReturn cr = CommandReturn.GOOD;
        try {
            hdfs = (FileSystem) env.getValue(Constants.HDFS);

            String dir = cmd.getArgs().length == 0 ? "/" : cmd.getArgs()[0];
            if (dir.startsWith("\"") & dir.endsWith("\"")) {
                dir = dir.substring(1, dir.length()-1);
            }
            logv(env, "CWD before: " + hdfs.getWorkingDirectory());
            logv(env, "CWD before (env): " + env.getRemoteWorkingDirectory());
            logv(env, "Requested CWD: " + dir);

            Path newPath = null;
            if (dir.startsWith("/")) {
                newPath = new Path(env.getProperties().getProperty(Constants.HDFS_URL), dir);
            } else {
//                newPath = new Path(hdfs.getWorkingDirectory(), dir);
                newPath = new Path(env.getRemoteWorkingDirectory(), dir);
            }

            Path qPath = newPath.makeQualified(hdfs);
            logv(env, "" + newPath);
            if (hdfs.getFileStatus(qPath).isDir() && hdfs.exists(qPath)) {
//                hdfs.setWorkingDirectory(qPath);
                env.setRemoteWorkingDirectory(qPath);
            } else {
                log(env, "No such directory: " + dir);
            }

        } catch (IOException e) {
            cr.setCode(CODE_CMD_ERROR);
            cr.getErr().print(e.getMessage());
//            cr.setDetails(e.getMessage());
//            cr = new CommandReturn(CODE_CMD_ERROR, e.getMessage());
        } finally {
            FSUtil.prompt(env);
        }
        return cr;
    }

    @Override
    protected String getDescription() {
        return "Remote Change Directory";
    }

    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, false);
    }

}
