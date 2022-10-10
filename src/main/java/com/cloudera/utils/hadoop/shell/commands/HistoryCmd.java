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

import java.util.ListIterator;

import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.shell.Environment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import jline.console.history.History.Entry;

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
    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {
        if (env.getConsoleReader() != null) {
            jline.console.history.History history = env.getConsoleReader().getHistory();
            ListIterator<Entry> it = history.entries();
            while (it.hasNext()) {
                Entry entry = it.next();
                System.out.println(entry.value());
            }
        } else {
            commandReturn.setCode(-1);
            commandReturn.getErr().print("No console reader defined.  Not available in 'api' mode");
//            return CommandReturn.BAD;
        }
        return commandReturn;
    }

}
