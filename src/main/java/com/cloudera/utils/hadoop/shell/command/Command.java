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

package com.cloudera.utils.hadoop.shell.command;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import jline.console.completer.Completer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.PrintStream;

public interface Command {

    String getDescription();
    String getHelpHeader();
    String getHelpFooter();
    String getUsage();
    
    String getName();

    void setErr(PrintStream err);
    void setOut(PrintStream out);

    CommandReturn execute(CliEnvironment env, CommandLine cmd, CommandReturn commandReturn);

//    void processCommandLine(CommandLine commandLine);

    Options getOptions();
    
    Completer getCompleter();

    CommandReturn implementation(CliEnvironment env, CommandLine cmd, CommandReturn commandReturn);
}
