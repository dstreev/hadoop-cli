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

package com.streever.hadoop.shell;

import com.streever.hadoop.hdfs.util.FileSystemOrganizer;
import com.streever.hadoop.hdfs.util.FileSystemState;
import com.streever.hadoop.shell.command.Command;
import jline.console.ConsoleReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Properties;
import java.util.Set;

public interface Environment {
    
    void addCommand(Command cmd);

    Command getCommand(String name);

    Set<String> commandList();
//    void setProperty(String key, String value);
//
//    String getProperty(String key);
//    String getProperty(String key, String default);

    Properties getProperties();
    void setValue(String key, Object value);
    Object getValue(String key);

    void setConfig(Configuration config);
    Configuration getConfig();

    FileSystemOrganizer getFileSystemOrganizer();

    void setPrompt(String prompt);
    String getPrompt();

//    String getCurrentPrompt();
//    void setCurrentPrompt(String prompt);
//    String getDefaultPrompt();
//    void setDefaultPrompt(String prompt);

//    Path getWorkingDirectory();
//    void setWorkingDirectory(Path workingDirectory);
//
//    Path getLocalWorkingDirectory();
//    void setLocalWorkingDirectory(Path workingDirectory);

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
