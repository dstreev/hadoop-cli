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

    enum Side {
        LEFT,RIGHT
    }

    protected Direction directionContext = null;

    protected int directives = 0;
    protected boolean directivesBefore = true;
    protected boolean directivesOptional = false;

    public HdfsAbstract(String name) {
        super(name);
    }

    public HdfsAbstract(String name, Environment env, Direction directionContext ) {
        super(name);
        this.env = env;
        this.directionContext = directionContext;
    }

    public HdfsAbstract(String name, Environment env, Direction directionContext, int directives ) {
        super(name);
        this.env = env;
        this.directionContext = directionContext;
        this.directives = directives;
    }

    public HdfsAbstract(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional ) {
        super(name);
        this.env = env;
        this.directionContext = directionContext;
        this.directives = directives;
        this.directivesBefore = directivesBefore;
        this.directivesOptional = directivesOptional;
    }

    public HdfsAbstract(String name, Environment env) {
        super(name);
        this.env = env;
    }

    protected String buildPath(Side side, String[] args, Direction context) {
        String rtn = null;

        FileSystem localfs = (FileSystem)env.getValue(Constants.LOCAL_FS);
        FileSystem hdfs = (FileSystem) env.getValue(Constants.HDFS);

        String in = null;

        switch (side) {
            case LEFT:
                if (args.length > 0)
                    if (directivesBefore) {
                        in = args[directives];
                    } else {
                        if (directivesOptional) {
                            if (args.length > directives) {
                                in = args[args.length-(directives+1)];
                            } else {
                                // in is null
                            }
                        } else {
                            in = args[args.length-(directives+1)];
                        }
                    }
                switch (context) {
                    case REMOTE_LOCAL:
                    case REMOTE_REMOTE:
                    case NONE:
                        rtn = buildPath2(hdfs.getWorkingDirectory().toString().substring(((String)env.getProperties().getProperty(Constants.HDFS_URL)).length()), in);
                        break;
                    case LOCAL_REMOTE:
                        rtn = buildPath2(localfs.getWorkingDirectory().toString().substring(5), in);
                        break;
                }
                break;
            case RIGHT:
                if (args.length > 1)
                    if (directivesBefore)
                        in = args[directives + 1];
                    else
                        in = args[args.length-(directives+1)];
                switch (context) {
                    case REMOTE_LOCAL:
                        rtn = buildPath2(localfs.getWorkingDirectory().toString().substring(5), in);
                        break;
                    case LOCAL_REMOTE:
                    case REMOTE_REMOTE:
                        rtn = buildPath2(hdfs.getWorkingDirectory().toString().substring(((String)env.getProperties().getProperty(Constants.HDFS_URL)).length()), in);
                        break;
                    case NONE:
                        break;
                }
                break;
        }
        if (rtn != null && rtn.contains(" ")) {
            rtn = "'" + rtn + "'";
        }
        return rtn;
    }

    protected String buildPath2(String current, String input) {
        if (input != null) {
            if (input.startsWith("/"))
                return input;
            else
                return current + "/" + input;
        } else {
            return current;
        }
    }


    @Override
    public Completer getCompleter() {
        return new FileSystemNameCompleter(this.env, false);
    }


}
