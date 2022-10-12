
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

package org.apache.hadoop.fs.shell;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;

import java.io.PrintStream;

/**
 * Extending to allow us to override the PrintStreams for native 'Commands'.
 */
public class CliCommandFactory extends CommandFactory {

    /** allows stdout to be captured if necessary */
    public PrintStream out = System.out;
    /** allows stderr to be captured if necessary */
    public PrintStream err = System.err;

    public CliCommandFactory() {
        super();
    }

    public CliCommandFactory(Configuration conf) {
        super(conf);
    }

    @Override
    public Command getInstance(String cmd) {
        Command rtn = super.getInstance(cmd);
        rtn.err = err;
        rtn.out = out;
        return rtn;
    }

    @Override
    public Command getInstance(String cmdName, Configuration conf) {
        Command rtn = super.getInstance(cmdName, conf);
        // Set / Reset these for this purpose.
        rtn.err = err;
        rtn.out = out;
        return rtn;
    }
}
