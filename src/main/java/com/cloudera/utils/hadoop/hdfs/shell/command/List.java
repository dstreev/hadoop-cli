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
package com.cloudera.utils.hadoop.hdfs.shell.command;

import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.shell.Environment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hadoop.shell.format.ANSIStyle;
import jline.console.completer.Completer;
import org.apache.commons.cli.CommandLine;

import java.util.Set;

public class List extends HdfsAbstract {

    public List(String name, Environment environment) {
        super(name, environment);

        // TODO: Setup Completer for "LIST"
    }

    @Override
    public String getDescription() {
        return "List available 'Namespaces'";
    }

    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {

        CommandReturn cr = new CommandReturn(0);

        StringBuilder sb = new StringBuilder();

        Set<String> lclNss = env.getFileSystemOrganizer().getNamespaces().keySet();

        for (String namespace : lclNss) {
            FileSystemState lclFss = env.getFileSystemOrganizer().getNamespaces().get(namespace);
            if (lclFss.equals(env.getFileSystemOrganizer().getCurrentFileSystemState())) {
                sb.append("*\t");
            } else {
                sb.append("\t");
            }
            if (lclFss.equals(env.getFileSystemOrganizer().getDefaultFileSystemState())) {
                sb.append(ANSIStyle.style(namespace, ANSIStyle.FG_BLUE, ANSIStyle.BLINK, ANSIStyle.UNDERSCORE)).append("\t");
            } else if (namespace.equalsIgnoreCase(Constants.LOCAL_FS)) {
                sb.append(ANSIStyle.style(namespace, ANSIStyle.FG_YELLOW)).append("\t");
            } else {
                sb.append(ANSIStyle.style(namespace, ANSIStyle.FG_RED)).append("\t");
            }
            sb.append(lclFss.toDisplay());
            sb.append("\n");
        }
        System.out.println(sb.toString());
        return cr;
    }

    @Override
    public Completer getCompleter() {
        return this.completer;
    }


}
