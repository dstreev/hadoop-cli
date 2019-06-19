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

import jline.console.ConsoleReader;
import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.streever.hadoop.hdfs.shell.completers.FileSystemNameCompleter;
import com.streever.tools.stemshell.Environment;

public class LocalCd extends HdfsCommand {
    private Environment env;

    public LocalCd(String name, Environment env) {
        super(name,env);
//        this.env = env;
    }

    public int execute(Environment env, CommandLine cmd, ConsoleReader reader) {
        try {

            FileSystem localfs = (FileSystem) env.getValue(Constants.LOCAL_FS);
            String dir = cmd.getArgs().length == 0 ? System
                            .getProperty("user.home") : cmd.getArgs()[0];
            logv(env, "Change Dir to: " + dir);
            logv(env, "CWD: " + localfs.getWorkingDirectory());
            Path newPath = null;
            if (dir.startsWith("~/")) {
                dir = System.getProperty("user.home") + dir.substring(1);
            }
            logv(env,"Dir: " + dir);
            newPath = new Path(dir);

            Path qPath = localfs.makeQualified(newPath);
            logv(env, "Qual Path: " + qPath);

            if (localfs.getFileStatus(qPath).isDir() && localfs.exists(qPath)) {
                localfs.setWorkingDirectory(qPath);
            }
            else {
                log(env, "No such directory: " + dir);
            }
            FSUtil.prompt(env);
        }
        catch (IOException e) {
            log(env, e.getMessage());
            return CODE_LOCAL_FS_ISSUE;
        }
        return CODE_SUCCESS;
    }

    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, true);
    }

}
