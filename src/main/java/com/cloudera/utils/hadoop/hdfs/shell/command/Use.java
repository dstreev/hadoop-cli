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
import org.apache.commons.cli.CommandLine;

import java.util.Iterator;
import java.util.Set;

public class Use extends HdfsAbstract {

    public Use(String name) {
        super(name);
        // TODO: Setup completer for 'use'
    }

    @Override
    public String getDescription() {
        return "Change current 'namespace'.  Use 'list' to review options.";
    }

    public CommandReturn implementation(CliSession session, CommandLine cmd, CommandReturn commandReturn) {
        String namespace = cmd.getArgs().length == 0 ? "" : cmd.getArgs()[0];
        CommandReturn cr = new CommandReturn(0);
        FileSystemOrganizer fso = session.getFileSystemOrganizer();
        if (namespace.equalsIgnoreCase("default")) {
            // reset to default namespace.
            namespace = fso.getDefaultFileSystemState().getNamespace();
        } else if (namespace.equalsIgnoreCase("alt")) {
            Set<String> namespaces = fso.getNamespaces().keySet();
            for (String lnamespace : namespaces) {
                FileSystemState lfss = fso.getFileSystemState(lnamespace);
                if (!lnamespace.equals(Constants.LOCAL_FS) && !lfss.equals(fso.getDefaultFileSystemState())) {
                    namespace = lnamespace;
                    break;
                }
            }
        }
        FileSystemState fss = fso.getFileSystemState(namespace);
        if (fss != null) {
            fso.setCurrentFileSystemState(fss);
            cr.setCode(0);
        } else {
            cr.setCode(-1);
            StringBuilder sb = new StringBuilder();
            Set<String> namespaces = fso.getNamespaces().keySet();
            for (Iterator i = namespaces.iterator(); i.hasNext(); ) {
                String ns = (String) i.next();
                FileSystemState sfss = fso.getFileSystemState(ns);
                if (sfss.equals(fso.getDefaultFileSystemState())) {
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

        return cr;
    }

}
