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

package com.cloudera.utils.hadoop.shell.command;

import com.cloudera.utils.hadoop.cli.CliEnvironment;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.PrintStream;

@Slf4j
public abstract class AbstractCommand implements Command{
    public static final int CODE_BAD_DATE = -321;
    public static final int CODE_SUCCESS = 0;
    public static final int CODE_LOCAL_FS_ISSUE = -123;
    public static final int CODE_NOT_CONNECTED = -10;
    public static final int CODE_CONNECTION_ISSUE = -11;
    public static final int CODE_CMD_ERROR = -1;
    public static final int CODE_PATH_ERROR = -20;
    public static final int CODE_FS_CLOSE_ISSUE = -100;
    public static final int CODE_STATS_ISSUE = -200;
    public static final int CODE_NOT_FOUND = 1;

    /** allows stdout to be captured if necessary */
    public PrintStream out = System.out;
    /** allows stderr to be captured if necessary */
    public PrintStream err = System.err;
    private final String name;

    protected Completer completer = new NullCompleter();

    public AbstractCommand(String name){
        this.name = name;
    }

    public String getHelpHeader() {
        StringBuilder sb = new StringBuilder();
        sb.append(getDescription()).append("\n");
        sb.append("Options:");
        return sb.toString();
    }

    public String getHelpFooter() {
        return null;
    }

    @Override
    public void setOut(PrintStream out) {
        this.out = out;
    }

    @Override
    public void setErr(PrintStream err) {
        this.err = err;
    }

    public String getName() {
        return name;
    }

    public Options getOptions() {
        Options options =  new Options();

        options.addOption("v", "verbose", false, "show verbose output");

        return options;
    }
    
    protected void processCommandLine(CommandLine commandLine) {
        // TODO: Handle Verbose here
        
//        if (commandLine.hasOption("buffer")) {
//            setBufferOutput(true);
//        } else {
//            setBufferOutput(false);
//        }
    }

    public String getUsage(){
        return getName() + " [Options ...] [Args ...]";
    }
    
    protected static void logv(CliEnvironment env, String log){
        if(env.isVerbose()){
            System.out.println(log);
        }
    }
//
    protected static void log(CliEnvironment env, String log){
        System.out.println(log);
    }
//
//    protected static void loge(CliEnvironment env, String log){
//        System.err.println(log);
//    }
//
    protected static void logd(CliEnvironment env, String log){
        if(env.isDebug()){
            System.out.println(log);
        }
    }

    public Completer getCompleter() {
        return this.completer;
    }
    
    @Override
    public CommandReturn execute(CliEnvironment env, CommandLine cmd, CommandReturn cr) {
        CommandReturn lclCr = cr;
        if (lclCr == null) {
            lclCr = new CommandReturn(CommandReturn.GOOD);
        }
//            PrintStream orig = System.out;
//            System.setOut(new PrintStream(lclCr.getBufferedOutputStream()));

            try {

                lclCr = implementation(env, cmd, lclCr);

//                System.out.flush();
            } catch (Throwable t) {
                log.error("Error in Command: {}", getName(), t);
//                t.printStackTrace();
            } finally {
                // Revert Buffered Output
//                System.setOut(orig);
            }
        return lclCr;
    }

    @Override
    public abstract CommandReturn implementation(CliEnvironment env, CommandLine cmdr, CommandReturn commandReturn);

}
