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

import java.util.ListIterator;

import com.streever.hadoop.shell.Environment;
import com.streever.hadoop.shell.command.AbstractCommand;
import com.streever.hadoop.shell.command.CommandReturn;
import jline.console.history.History.Entry;

import org.apache.commons.cli.CommandLine;

public class HistoryCmd extends AbstractCommand {

    public HistoryCmd(String name) {
        super(name);
    }

    @Override
    protected String getDescription() {
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
