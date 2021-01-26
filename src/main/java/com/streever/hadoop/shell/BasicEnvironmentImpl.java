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
import com.streever.hadoop.shell.command.Command;
import jline.console.ConsoleReader;
import org.apache.hadoop.conf.Configuration;

import java.util.*;

public class BasicEnvironmentImpl implements Environment {

    private String defaultPrompt = "basic:$";
    private String currentPrompt = null;
//    private Path remoteWorkingDirectory = new Path("/");
//    private Path localWorkingDirectory = new Path("/");

    private Boolean verbose = Boolean.FALSE;
    private Boolean debug = Boolean.FALSE;
    private Boolean silent = Boolean.FALSE;
    private Boolean apiMode = Boolean.FALSE;

    private Configuration config = null;
    private FileSystemOrganizer fileSystemOrganizer = null;

    private ConsoleReader consoleReader = null;

    private Properties props = new Properties();
    private HashMap<String, Object> values = new HashMap<String, Object>();

    private Map<String, Command> commands = new TreeMap<String, Command>();

    public void addCommand(Command cmd) {
        this.commands.put(cmd.getName(), cmd);
    }

    @Override
    public void setConfig(Configuration config) {
        this.config = config;
        this.fileSystemOrganizer = new FileSystemOrganizer(config);
    }

    @Override
    public Configuration getConfig() {
        return config;
    }

    @Override
    public FileSystemOrganizer getFileSystemOrganizer() {
        return this.fileSystemOrganizer;
    }

    public void setPrompt(String prompt) {
        this.currentPrompt = prompt;
    }

    @Override
    public String getPrompt() {
        return getFileSystemOrganizer().getPrompt();
//        return currentPrompt == null?currentPrompt:defaultPrompt;
    }

//    @Override
//    public Path getWorkingDirectory() {
//        return null;
//    }
//
//    @Override
//    public void setWorkingDirectory(Path workingDirectory) {
//
//    }

    public Command getCommand(String name) {
        return this.commands.get(name);
    }

    public Set<String> commandList() {
        return this.commands.keySet();
    }

    @Override
    public ConsoleReader getConsoleReader() {
        return consoleReader;
    }

    public void setConsoleReader(ConsoleReader consoleReader) {
        this.consoleReader = consoleReader;
    }

    public Properties getProperties() {
        return this.props;
    }

    public void setValue(String key, Object value) {
        this.values.put(key, value);
    }

    public Object getValue(String key) {
        return this.values.get(key);
    }


//    public String getDefaultPrompt() {
//        return this.defaultPrompt;
//    }
//
//    public void setDefaultPrompt(String prompt) {
//        this.defaultPrompt = prompt;
//    }
//
//    @Override
//    public String getCurrentPrompt() {
//        return currentPrompt;
//    }
//
//    @Override
//    public void setCurrentPrompt(String currentPrompt) {
//        this.currentPrompt = currentPrompt;
//    }

//    @Override
//    public Path getRemoteWorkingDirectory() {
//        return remoteWorkingDirectory;
//    }
//
//    @Override
//    public void setRemoteWorkingDirectory(Path remoteWorkingDirectory) {
//        this.remoteWorkingDirectory = remoteWorkingDirectory;
//    }

//    @Override
//    public Path getLocalWorkingDirectory() {
//        return localWorkingDirectory;
//    }
//
//    @Override
//    public void setLocalWorkingDirectory(Path localWorkingDirectory) {
//        this.localWorkingDirectory = localWorkingDirectory;
//    }

    public Boolean isVerbose() {
        return verbose;
    }

    @Override
    public void setVerbose(Boolean verbose) {
        this.verbose = verbose;
    }

    @Override
    public Boolean isDebug() {
        return debug;
    }

    @Override
    public void setDebug(Boolean debug) {
        this.debug = debug;
    }

    public Boolean isSilent() {
        return silent;
    }

    @Override
    public void setSilent(Boolean silent) {
        this.silent = silent;
    }

    @Override
    public Boolean isApiMode() {
        return apiMode;
    }

    @Override
    public void setApiMode(Boolean apiMode) {
        this.apiMode = apiMode;
    }
}
