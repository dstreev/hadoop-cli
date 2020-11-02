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

package com.streever.hadoop.shell.command;

import com.streever.hadoop.shell.Environment;
import jline.console.completer.Completer;
import jline.console.completer.NullCompleter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.PrintStream;

public abstract class AbstractCommand implements Command{
    public static int CODE_BAD_DATE = -321;
    public static int CODE_SUCCESS = 0;
    public static int CODE_LOCAL_FS_ISSUE = -123;
    public static int CODE_NOT_CONNECTED = -10;
    public static int CODE_CONNECTION_ISSUE = -11;
    public static int CODE_CMD_ERROR = -1;
    public static int CODE_PATH_ERROR = -20;
    public static int CODE_FS_CLOSE_ISSUE = -100;
    public static int CODE_STATS_ISSUE = -200;
    public static int CODE_NOT_FOUND = 1;

    /** allows stdout to be captured if necessary */
    public PrintStream out = System.out;
    /** allows stderr to be captured if necessary */
    public PrintStream err = System.err;
    private String name;

    protected Completer completer = new NullCompleter();

    public AbstractCommand(String name){
        this.name = name;
    }

    protected abstract String getDescription();

    public String getHelpHeader() {
        StringBuilder sb = new StringBuilder();
        sb.append(getDescription()).append("\n");
        sb.append("Options:");
        return sb.toString();
    }

    public String gethelpFooter() {
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
    
    protected static void logv(Environment env, String log){
        if(env.isVerbose()){
            System.out.println(log);
        }
    }
    
    protected static void log(Environment env, String log){
        System.out.println(log);
    }

    protected static void loge(Environment env, String log){
        System.err.println(log);
    }

    protected static void logd(Environment env, String log){
        if(env.isDebug()){
            System.out.println(log);
        }
    }

    public Completer getCompleter() {
        return this.completer;
    }
    
    @Override
    public CommandReturn execute(Environment env, CommandLine cmd, CommandReturn cr) {
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
                t.printStackTrace();
            } finally {
                // Revert Buffered Output
//                System.setOut(orig);
            }
        return lclCr;
    }

    @Override
    public abstract CommandReturn implementation(Environment env, CommandLine cmdr, CommandReturn commandReturn);

}
