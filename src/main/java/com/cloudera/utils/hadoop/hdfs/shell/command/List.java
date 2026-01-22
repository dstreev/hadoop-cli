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

import com.cloudera.utils.hadoop.cli.CliSession;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemOrganizer;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemState;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.cloudera.utils.hadoop.shell.format.ANSIStyle;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;

import java.util.Set;

@Slf4j
public class List extends HdfsAbstract {

    public List(String name) {
        super(name);
        // TODO: Setup Completer for "LIST"
    }

    @Override
    public String getDescription() {
        return "List available 'Namespaces'";
    }

    public CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn) {
        log.debug("List Namespaces");
        FileSystemOrganizer fso = session.getFileSystemOrganizer();
        CommandReturn cr = new CommandReturn(0);

        StringBuilder sb = new StringBuilder();

        Set<String> lclNss = fso.getNamespaces().keySet();

        for (String namespace : lclNss) {
            FileSystemState lclFss = fso.getNamespaces().get(namespace);
            if (lclFss.equals(fso.getCurrentFileSystemState())) {
                log.debug("Current Namespace: {}", namespace);
                sb.append("*\t");
            } else {
                log.debug("Namespace: {}", namespace);
                sb.append("\t");
            }
            if (lclFss.equals(fso.getDefaultFileSystemState())) {
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
        return new NullCompleter();
    }


}
