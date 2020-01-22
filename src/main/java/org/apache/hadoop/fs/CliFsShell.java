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

package org.apache.hadoop.fs;

import org.apache.hadoop.fs.shell.CliCommandFactory;
import org.apache.hadoop.conf.Configuration;
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
