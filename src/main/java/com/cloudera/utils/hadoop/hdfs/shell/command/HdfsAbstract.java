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
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public abstract class HdfsAbstract extends AbstractCommand {

    protected PathDirectives pathDirectives;

    public HdfsAbstract(String name) {
        super(name);
    }

    public HdfsAbstract(String name, Direction directionContext) {
        super(name);
        pathDirectives = new PathDirectives(directionContext);
    }

    public HdfsAbstract(String name, Direction directionContext, int directives) {
        super(name);
        pathDirectives = new PathDirectives(directionContext, directives);
    }

    public HdfsAbstract(String name, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name);
        pathDirectives = new PathDirectives(directionContext, directives, directivesBefore, directivesOptional);
    }

    protected PathBuilder getPathBuilder(CliSession session) {
        if (pathDirectives != null) {
            return new PathBuilder(session, pathDirectives);
        } else {
            return new PathBuilder(session);
        }
    }

    @Override
    public Options getOptions() {
        Options options = super.getOptions();
        return options;
    }

    protected void processCommandLine(CommandLine commandLine) {
        super.processCommandLine(commandLine);
    }
    
//    @Override
//    public Completer getCompleter() {
//        return new FileSystemNameCompleter(this.env, false);
//    }
}
