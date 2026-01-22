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

package com.cloudera.utils.hadoop.shell.commands;

import com.cloudera.utils.hadoop.cli.CliSession;
import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;

import org.apache.commons.cli.CommandLine;

public class HistoryCmd extends AbstractCommand {

    public HistoryCmd(String name) {
        super(name);
    }

    @Override
    public String getDescription() {
        return "Commandline History";
    }

    @Override
    public CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn) {
        // History is now managed externally - this command is kept for compatibility
        commandReturn.setCode(-1);
        commandReturn.getErr().print("History command is not available in the current session mode");
        return commandReturn;
    }

}
