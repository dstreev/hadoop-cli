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

import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hadoop.shell.format.ANSIStyle;
import jline.console.completer.Completer;
import org.apache.commons.cli.CommandLine;

import java.util.Set;

public class List extends HdfsAbstract {

    public List(String name, CliEnvironment cliEnvironment) {
        super(name, cliEnvironment);

        // TODO: Setup Completer for "LIST"
    }

    @Override
    public String getDescription() {
        return "List available 'Namespaces'";
    }

    public CommandReturn implementation(CliEnvironment env, CommandLine cmd, CommandReturn commandReturn) {

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
