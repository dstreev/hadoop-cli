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

package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.CliCommandFactory;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Extending to allow us to override the PrintStreams for native 'Commands'.
 */
public class CliFsShell extends FsShell {

    /** allows stdout to be captured if necessary */
    private PrintStream out = System.out;
    /** allows stderr to be captured if necessary */
    private PrintStream err = System.err;

    public PrintStream getOut() {
        return out;
    }

    public void setOut(PrintStream out) {
        this.out = out;
        if (commandFactory != null) {
            ((CliCommandFactory)commandFactory).out = out;
        }
    }

    public PrintStream getErr() {
        return err;
    }

    public void setErr(PrintStream err) {
        this.err = err;
        if (commandFactory != null) {
            ((CliCommandFactory)commandFactory).err = err;
        }
    }

    public CliFsShell() {
        super();
    }

    public CliFsShell(Configuration conf) {
        super(conf);
    }


    @Override
    public void init() throws IOException {
        getConf().setQuietMode(true);
        UserGroupInformation.setConfiguration(getConf());
        if (commandFactory == null) {
            commandFactory = new CliCommandFactory(getConf());
            ((CliCommandFactory)commandFactory).out = out;
            ((CliCommandFactory)commandFactory).err = err;
            // Inner Protected Classes of FsShell
            commandFactory.addObject(new Help(), "-help");
            commandFactory.addObject(new Usage(), "-usage");
            registerCommands(commandFactory);
        }
    }

    @Override
    protected void registerCommands(CommandFactory factory) {
        if (this.getClass().equals(CliFsShell.class)) {
            factory.registerCommands(FsCommand.class);
        }

    }

    @Override
    public int run(String[] argv) throws Exception {
        return super.run(argv);
    }
}
