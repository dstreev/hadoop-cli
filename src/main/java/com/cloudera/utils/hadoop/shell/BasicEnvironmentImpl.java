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

import com.cloudera.utils.hadoop.shell.command.Command;
import com.cloudera.utils.hadoop.hdfs.util.FileSystemOrganizer;
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
