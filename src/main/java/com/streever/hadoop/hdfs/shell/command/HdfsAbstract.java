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
import com.streever.tools.stemshell.Environment;
import com.streever.tools.stemshell.command.AbstractCommand;
import jline.console.completer.Completer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public abstract class HdfsAbstract extends AbstractCommand {

    public int CODE_BAD_DATE = -321;
    public int CODE_SUCCESS = 0;
    public int CODE_LOCAL_FS_ISSUE = -123;
    public int CODE_NOT_CONNECTED = -10;
    public int CODE_CONNECTION_ISSUE = -11;
    public int CODE_CMD_ERROR = -1;
    public int CODE_PATH_ERROR = -20;
    public int CODE_FS_CLOSE_ISSUE = -100;
    public int CODE_STATS_ISSUE = -200;
    public int CODE_NOT_FOUND = -99;
    
    protected Environment env;
    
    protected PathBuilder pathBuilder;
    protected PathDirectives pathDirectives;

    public HdfsAbstract(String name) {
        super(name);
    }

    public HdfsAbstract(String name, Environment env, Direction directionContext ) {
        super(name);
        pathDirectives = new PathDirectives(directionContext);
        pathBuilder = new PathBuilder(env, pathDirectives);
        this.env = env;
    }

    public HdfsAbstract(String name, Environment env, Direction directionContext, int directives ) {
        super(name);
        this.env = env;
        pathDirectives = new PathDirectives(directionContext, directives);
        pathBuilder = new PathBuilder(env, pathDirectives);
    }

    public HdfsAbstract(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional ) {
        super(name);
        this.env = env;
        pathDirectives = new PathDirectives(directionContext, directives, directivesBefore, directivesOptional);
        pathBuilder = new PathBuilder(env, pathDirectives);
    }

    public HdfsAbstract(String name, Environment env) {
        super(name);
        this.env = env;
    }

    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, false);
    }
}
