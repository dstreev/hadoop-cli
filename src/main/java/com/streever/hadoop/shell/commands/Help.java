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

package com.streever.hadoop.shell.commands;

import com.streever.hadoop.hdfs.shell.completers.FileSystemNameCompleter;
import com.streever.hadoop.shell.Environment;
import com.streever.hadoop.shell.command.AbstractCommand;
import com.streever.hadoop.shell.command.Command;
import com.streever.hadoop.shell.command.CommandReturn;
import com.streever.hadoop.shell.format.ANSIStyle;
import jline.console.completer.*;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.lang.StringUtils;

public class Help extends AbstractCommand {
    private Environment env;

    public Help(String name, Environment env) {
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

    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {
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
