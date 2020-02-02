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

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.jcabi.manifests.Manifests;
import com.streever.hadoop.shell.command.AbstractCommand;
import com.streever.hadoop.shell.command.CommandReturn;
import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;
import jline.console.history.History;

import org.apache.commons.cli.*;
import org.fusesource.jansi.AnsiConsole;

import com.streever.hadoop.shell.command.Command;

public abstract class AbstractShell implements Shell {
    private CommandLineParser parser = new PosixParser();
    private Environment env = null; //new Environment();
    private String bannerResource = "/banner.txt";
    private boolean apiMode = false;

    protected Environment getEnv() {
        return env;
    }

    protected void setEnv(Environment env) {
        this.env = env;
    }

    public boolean isApiMode() {
        return apiMode;
    }

    public void setApiMode(boolean apiMode) {
        this.apiMode = apiMode;
        getEnv().setApiMode(true);
    }

    public String getBannerResource() {
        return bannerResource;
    }

    public void setBannerResource(String bannerResource) {
        this.bannerResource = bannerResource;
    }

    protected static void logv(Environment env, String log) {
        if (env.isVerbose()) {
            System.out.println(log);
        }
    }

    protected static void log(Environment env, String log) {
        System.out.println(log);
    }

    protected static void loge(Environment env, String log) {
//        if (!env.isApiMode())
            System.err.println(log);
    }

    protected abstract boolean preProcessInitializationArguments(String[] arguments);

    protected abstract boolean postProcessInitializationArguments(String[] arguments);

    protected boolean setupEnvironment(String[] args) throws Exception {
        if (!preProcessInitializationArguments(args)) {
            loge(env, "Initialization Issue");
            return false;
        }

        initialize();

        // if the subclass hasn't defined a prompt, do so for them.
        if (getEnv().getDefaultPrompt() == null) {
            getEnv().setDefaultPrompt("$");
        }

        // banner
        if (!getEnv().isSilent()) {
            InputStream is = this.getClass().getResourceAsStream(getBannerResource());
            if (is != null) {
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                String line = null;
                while ((line = br.readLine()) != null) {
                    // Replace Token.
                    log(env, substituteVariables(line));
                }
            }
        }

        if (postProcessInitializationArguments(args)) {
            // create reader and add completers
            return true;
        } else {
            loge(env, "Initialization Issue.");
            processInput("exit");
            return false;
        }

    }

    public final void start(String[] arguments) throws Exception {
        if (!setupEnvironment(arguments)) {
            loge(env, "Initialization Issue");
            return;
        } else {
            // create reader and add completers
            if (!apiMode) {
                ConsoleReader reader = new ConsoleReader();
                getEnv().setConsoleReader(reader);

                reader.addCompleter(initCompleters(env));
                // add history support
                reader.setHistory(initHistory());

                AnsiConsole.systemInstall();

                acceptCommands(reader);
            }
        }

    }

    public static String substituteVariables(String template) {
        Pattern pattern = Pattern.compile("\\$\\{(.+?)\\}");
        Matcher matcher = pattern.matcher(template);
        // StringBuilder cannot be used here because Matcher expects StringBuffer
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String matchStr = matcher.group(1);
//            System.out.println("Found variable: " + matchStr);
            try {
                String replacement = Manifests.read(matchStr);
                if (replacement != null) {
//                    System.out.println("Replacement Value: " + replacement);
                    // quote to work properly with $ and {,} signs
                    matcher.appendReplacement(buffer, replacement != null ? Matcher.quoteReplacement(replacement) : "null");
                } else {
//                    System.out.println("No replacement found for: " + matchStr);
                }
            } catch (IllegalArgumentException iae) {
                //iae.printStackTrace();
                // Couldn't locate MANIFEST Entry.
                // Silently continue. Usually happens in IDE->run.
            }
        }
        matcher.appendTail(buffer);
        String rtn = buffer.toString();
        return rtn;
    }

    public CommandReturn processInput(String line) {
        // Check for Pipelining.
        // Pipelining are used to string commands together.
        // https://en.wikipedia.org/wiki/Pipeline_%28Unix%29
        /*
        This pipelining works a bit different in the sense the downstream function does
        NOT take a stream.  So we need to iterate through the previous functions output
        and repeatively call the next function in the pipeline.

        At this time, the pipeline only support 1 redirect.
         */
        String splitRegEx = "\\|(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)";
        String[] cmds = line.split(splitRegEx);
        if (cmds.length > 2) {
            CommandReturn crLength = new CommandReturn(CommandReturn.BAD);
            crLength.getErr().print("Only support single depth pipeline at this time.");
            return crLength;
        }
        CommandReturn previousCR = null;
        for (String command : cmds) {
            if (previousCR == null) {
                // First time thru
                previousCR = processCommand(command, null);
            } else {
                BufferedReader bufferedReader = new BufferedReader(new StringReader(previousCR.getReturn()));
                CommandReturn innerCR = new CommandReturn(CommandReturn.GOOD);
                while (true) {
                    try {
                        if (!((line = bufferedReader.readLine()) != null)) break;
                    } catch (IOException e) {
//                        e.printStackTrace();
                        break;
                    }
                    // Check line for spaces.  If it has them, quote it.
                    String adjustedLine = null;
                    if (line.contains(" ")) {
                        adjustedLine = "\"" + line + "\"";
                    } else {
                        adjustedLine = line;
                    }
                    String pipedCommand = command.trim() + " " + adjustedLine;
                    innerCR = processCommand(pipedCommand, innerCR);
                }
                previousCR = innerCR;
            }
        }
        return previousCR;
    }

    protected CommandReturn processCommand(String line, CommandReturn commandReturn) {
        // Deal with args that are in quotes and don't split them.
//        String[] argv = line.split("\\s+(?=((\\\\[\\\\\"]|[^\\\\\"])*\"(\\\\[\\\\\"]|[^\\\\\"])*\")*(\\\\[\\\\\"]|[^\\\\\"])*$)");
//        String[] argv = line.split("[^\\s\\\"']+|\\\"([^\\\"]*)\\\"|'([^']*)'");
//        String cmdName = argv[0];
        CommandReturn cr = commandReturn;
        if (cr == null) {
            cr = new CommandReturn(CommandReturn.GOOD);
        }

        List<String> matchList = new ArrayList<String>();
        Pattern regex = Pattern.compile("[^\\s\"']+|\"([^\"]*)\"|'([^']*)'");
        Matcher regexMatcher = regex.matcher(line);
        while (regexMatcher.find()) {
            if (regexMatcher.group(1) != null) {
                // Add double-quoted string without the quotes
                matchList.add(regexMatcher.group(1));
            } else if (regexMatcher.group(2) != null) {
                // Add single-quoted string without the quotes
                matchList.add(regexMatcher.group(2));
            } else {
                // Add unquoted word
                matchList.add(regexMatcher.group());
            }
        }

        String[] argv = new String[matchList.size()];
        matchList.toArray(argv);

//        CommandReturn cr = null;

        if (matchList.size() == 0) {
            cr.setCode(AbstractCommand.CODE_CMD_ERROR);
            cr.getErr().print("Match List = 0");
//            cr.setDetails("Match List = 0");
//            cr = CommandReturn.BAD;
            return cr;
        }

        String cmdName = argv[0];

//        cr = new CommandReturn(0);

        Command command = env.getCommand(cmdName);


        if (command != null) {
            // Set Command io.
            command.setErr(cr.getErr());
            command.setOut(cr.getOut());

            String[] cmdArgs = null;
            if (argv.length > 1) {
                cmdArgs = Arrays.copyOfRange(argv, 1, argv.length);
            }
            CommandLine cl = parse(command, cmdArgs);
            if (cl != null) {
                try {
                    cr = command.execute(env, cl, cr);
//                    if (cr.isError()) {
//                        loge(env, cr.getError());
//                    } else {
//                        logv(env, cr.getReturn());
//                    }
                } catch (Throwable e) {
//                    e.printStackTrace();
                    loge(getEnv(), "Command failed with error: "
                            + e.getMessage());
                    if (cl.hasOption("v")) {
                        loge(getEnv(), e.getMessage());
                    }
                } finally {
                }
            }

        } else {
            if (cmdName != null && cmdName.length() > 0) {
                loge(env, cmdName + ": command not found");
            }
        }
        return cr;
    }

    private void acceptCommands(ConsoleReader reader) throws IOException {
        String line;
        while ((line = reader.readLine(getEnv().getCurrentPrompt() + " ")) != null) {
            CommandReturn cr = processInput(line);
            if (!cr.isError()) {
                System.out.println(cr.getReturn());
            } else {
                System.err.println("ERROR   CODE : " + cr.getCode());
                System.err.println("     Command : " + cr.getCommand());
                System.err.println("       ERROR : " + cr.getError());
            }
        }
    }

    private CommandLine parse(Command cmd, String[] args) {
        Options opts = cmd.getOptions();
        CommandLine retval = null;
        try {
            retval = parser.parse(opts, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
        }
        return retval;
    }

    private Completer initCompleters(Environment env) {
        // create completers
        ArrayList<Completer> completers = new ArrayList<Completer>();
        for (String cmdName : env.commandList()) {
            // command name
            StringsCompleter sc = new StringsCompleter(cmdName);

            ArrayList<Completer> cmdCompleters = new ArrayList<Completer>();
            // add a completer for the command name
            cmdCompleters.add(sc);
            // add the completer for the command
            cmdCompleters.add(env.getCommand(cmdName).getCompleter());
            // add a terminator for the command
            // cmdCompleters.add(new NullCompleter());

            ArgumentCompleter ac = new ArgumentCompleter(cmdCompleters);
            completers.add(ac);
        }

        AggregateCompleter aggComp = new AggregateCompleter(completers);

        return aggComp;
    }

    private History initHistory() throws IOException {
        File dir = new File(System.getProperty("user.home"), "."
                + this.getName());
        if (dir.exists() && dir.isFile()) {
            throw new IllegalStateException(
                    "Default configuration file exists and is not a directory: "
                            + dir.getAbsolutePath());
        } else if (!dir.exists()) {
            dir.mkdir();
        }
        // directory created, touch history file
        File histFile = new File(dir, "history");
        if (!histFile.exists()) {
            if (!histFile.createNewFile()) {
                throw new IllegalStateException(
                        "Unable to create history file: "
                                + histFile.getAbsolutePath());
            }
        }

        final FileHistory hist = new FileHistory(histFile);

        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                try {
                    hist.flush();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        });

        return hist;

    }

    public abstract String getName();

}
