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

import com.streever.hadoop.hdfs.util.FileSystemState;
import com.streever.hadoop.shell.Environment;
import com.streever.hadoop.shell.command.CommandReturn;
import com.streever.hadoop.shell.format.ANSIStyle;
import jline.console.completer.Completer;
import org.apache.commons.cli.CommandLine;

import java.util.Iterator;
import java.util.Set;

public class Use extends HdfsAbstract {

    public Use(String name, Environment environment) {
        super(name, environment);
    }

    @Override
    public String getDescription() {
        return "Change current 'namespace'.  Use 'list' to review options.";
    }

    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {
        String namespace = cmd.getArgs().length == 0 ? "" : cmd.getArgs()[0];
        CommandReturn cr = new CommandReturn(0);
        if (namespace.equalsIgnoreCase("default")) {
            // reset to default namespace.
            namespace = env.getFileSystemOrganizer().getDefaultFileSystemState().getNamespace();
        } else if (namespace.equalsIgnoreCase("alt")) {
            Set<String> namespaces = env.getFileSystemOrganizer().getNamespaces().keySet();
            for (String lnamespace : namespaces) {
                FileSystemState lfss = env.getFileSystemOrganizer().getFileSystemState(lnamespace);
                if (!lnamespace.equals(Constants.LOCAL_FS) && !lfss.equals(env.getFileSystemOrganizer().getDefaultFileSystemState())) {
                    namespace = lnamespace;
                    break;
                }
            }
        }
        FileSystemState fss = env.getFileSystemOrganizer().getFileSystemState(namespace);
        if (fss != null) {
            env.getFileSystemOrganizer().setCurrentFileSystemState(fss);
            cr.setCode(0);
        } else {
            cr.setCode(-1);
            StringBuilder sb = new StringBuilder();
            Set<String> namespaces = env.getFileSystemOrganizer().getNamespaces().keySet();
            for (Iterator i = namespaces.iterator(); i.hasNext(); ) {
                String ns = (String) i.next();
                FileSystemState sfss = env.getFileSystemOrganizer().getFileSystemState(ns);
                if (sfss.equals(env.getFileSystemOrganizer().getDefaultFileSystemState())) {
                    sb.append(ns + "(default)");
                } else if (ns.equals(Constants.LOCAL_FS)) {
                    sb.append("LOCAL");
                } else {
                    sb.append(ns + "(alt)");
                }
                if (i.hasNext()) {
                    sb.append(", ");
                }
            }
            cr.setError("Namespace '" + namespace + "' not found. Available options: " +
                    ANSIStyle.style(sb.toString(), ANSIStyle.BOLD, ANSIStyle.UNDERSCORE, ANSIStyle.FG_BLUE));
        }

//            FSUtil.prompt(env);

        return cr;
    }

    @Override
    public Completer getCompleter() {
        return this.completer;
    }


}
