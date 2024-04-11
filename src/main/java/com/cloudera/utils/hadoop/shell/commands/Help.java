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

import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.Command;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hadoop.shell.format.ANSIStyle;
import jline.console.completer.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.lang3.StringUtils;

public class Help extends AbstractCommand {
    private CliEnvironment env;

    public Help(String name, CliEnvironment env) {
        super(name);
        this.env = env;
    }

    @Override
    public Completer getCompleter() {
        StringsCompleter strCompleter = new StringsCompleter(this.env.commandList());
        NullCompleter nullCompleter = new NullCompleter();
        Completer completer = new AggregateCompleter(strCompleter, nullCompleter);

        return completer;
    }

    @Override
    public String getDescription() {
        return "Help";
    }

    public CommandReturn implementation(CliEnvironment env, CommandLine cmd, CommandReturn commandReturn) {
        log(env, "----------------------------------------------------");
        log(env, "Command Listing.  Use 'help <cmd>' for detailed help");
        log(env, "----------------------------------------------------");
        if (cmd.getArgs().length == 0) {
            for (String str : env.commandList()) {
                log(env, StringUtils.rightPad(ANSIStyle.style(str, ANSIStyle.BOLD, ANSIStyle.FG_GREEN), 30) +
                        "\t\t" + ANSIStyle.style(env.getCommand(str).getDescription(), ANSIStyle.FG_YELLOW));
//                log(env, str);
            }
        } else {
            Command command = env.getCommand(cmd.getArgs()[0]);
            logv(env, "Get Help for command: " + command.getName() + "(" + command.getClass().getName() + ")");
            printHelp(command);
        }
        return commandReturn;
    }
    
    private void printHelp(Command cmd){
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp(cmd.getUsage(), cmd.getHelpHeader(), cmd.getOptions(), cmd.gethelpFooter());
    }
}
