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

package com.cloudera.utils.hadoop.shell;

import com.cloudera.utils.hadoop.hdfs.util.FileSystemOrganizer;
import com.cloudera.utils.hadoop.shell.command.Command;
import jline.console.ConsoleReader;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;
import java.util.Set;

public interface Environment {
    
    void addCommand(Command cmd);

    Command getCommand(String name);

    Set<String> commandList();

    Properties getProperties();
    void setValue(String key, Object value);
    Object getValue(String key);

    void setConfig(Configuration config);
    Configuration getConfig();

    FileSystemOrganizer getFileSystemOrganizer();

    void setPrompt(String prompt);
    String getPrompt();

    Boolean isVerbose();
    void setVerbose(Boolean verbose);

    void setDebug(Boolean debug);
    Boolean isDebug();

    Boolean isSilent();
    void setSilent(Boolean verbose);

    Boolean isApiMode();
    void setApiMode(Boolean apiMode);

    ConsoleReader getConsoleReader();
    void setConsoleReader(ConsoleReader consoleReader);
}
