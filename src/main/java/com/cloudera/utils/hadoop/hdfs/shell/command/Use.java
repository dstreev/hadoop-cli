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
import org.apache.commons.cli.CommandLine;

import java.util.Iterator;
import java.util.Set;

public class Use extends HdfsAbstract {

    public Use(String name, CliEnvironment cliEnvironment) {
        super(name, cliEnvironment);

        // TODO: Setup completer for 'use'

    }

    @Override
    public String getDescription() {
        return "Change current 'namespace'.  Use 'list' to review options.";
    }

    public CommandReturn implementation(CliEnvironment env, CommandLine cmd, CommandReturn commandReturn) {
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
            // Was hoping this would help completion on 'non' defaultFS.  It didn't.
            /*
            if (fss.getProtocol().startsWith("hdfs://") || fss.getProtocol().startsWith("ofs://")) {
                env.getConfig().set("fs.defaultFS", fss.getURI());
            }
            */
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

}
