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

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public abstract class HdfsAbstract extends AbstractCommand {

    protected CliEnvironment env;
    
    protected PathBuilder pathBuilder;
    protected PathDirectives pathDirectives;

    public HdfsAbstract(String name) {
        super(name);
    }

    public HdfsAbstract(String name, CliEnvironment env, Direction directionContext ) {
        super(name);
        pathDirectives = new PathDirectives(directionContext);
        pathBuilder = new PathBuilder(env, pathDirectives);
        this.env = env;
    }

    public HdfsAbstract(String name, CliEnvironment env, Direction directionContext, int directives ) {
        super(name);
        this.env = env;
        pathDirectives = new PathDirectives(directionContext, directives);
        pathBuilder = new PathBuilder(env, pathDirectives);
    }

    public HdfsAbstract(String name, CliEnvironment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional ) {
        super(name);
        this.env = env;
        pathDirectives = new PathDirectives(directionContext, directives, directivesBefore, directivesOptional);
        pathBuilder = new PathBuilder(env, pathDirectives);
    }

    public HdfsAbstract(String name, CliEnvironment env) {
        super(name);
        this.env = env;
        this.pathBuilder = new PathBuilder(env);
    }

    public CliEnvironment getEnv() {
        return env;
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
