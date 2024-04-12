/*
 * Copyright (c) 2022-2024. David W. Streever All Rights Reserved
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

package com.cloudera.utils.hadoop.cli;

import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import com.jcabi.manifests.Manifests;
import jline.console.ConsoleReader;
import jline.console.completer.AggregateCompleter;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;
import jline.console.history.History;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.jansi.AnsiConsole;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.cloudera.utils.hadoop.cli.CliEnvironment.*;

@Component
@Slf4j
@Getter
@Setter
public class Shell {

    private String bannerResource = "/hadoop_banner_0.txt";

    protected static void screen(String log) {
         System.out.println(log);
    }

    protected static void screene(String log) {
            System.err.println(log);
    }

    public void startShell(CliEnvironment cliEnvironment) throws Exception, DisabledException {
                // banner
        if (!cliEnvironment.isSilent()) {
            InputStream is = this.getClass().getResourceAsStream(getBannerResource());
            if (is != null) {
                BufferedReader br = new BufferedReader(new InputStreamReader(is));
                String line = null;
                while ((line = br.readLine()) != null) {
                    // Replace Token.
                    screen(substituteVariablesFromManifest(line));
                }
            }
        }


        ConsoleReader reader = new ConsoleReader();
        cliEnvironment.setConsoleReader(reader);

//                initCompleters(reader, env);
        reader.addCompleter(initCompleters(cliEnvironment));

        // add history support
        reader.setHistory(initHistory());

        AnsiConsole.systemInstall();
//        cliEnvironment.processInput("connect");

        acceptCommands(reader, cliEnvironment);

    }

    public static String substituteVariablesFromManifest(String template) {
        Pattern pattern = Pattern.compile("\\$\\{(.+?)\\}");
        Matcher matcher = pattern.matcher(template);
        // StringBuilder cannot be used here because Matcher expects StringBuffer
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String matchStr = matcher.group(1);
            try {
                String replacement = Manifests.read(matchStr);
                if (replacement != null) {
                    // quote to work properly with $ and {,} signs
                    matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
                }
            } catch (IllegalArgumentException iae) {
                //iae.printStackTrace();
                // Couldn't locate MANIFEST Entry.
                // Silently continue. Usually happens in IDE->run.
            }
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private void acceptCommands(ConsoleReader reader, CliEnvironment cliEnvironment) throws IOException, DisabledException {
        String line;
        while ((line = reader.readLine(cliEnvironment.getPrompt() + " ")) != null) {
            if (!line.trim().isEmpty()) {
                CommandReturn cr = cliEnvironment.processInput(line);
                if (!cr.isError()) {
                    if (cr.getReturn() != null) {
                        if (!cr.getStyles().isEmpty()) {
                            String out = cr.getStyledReturn();
                            screen(out);
                        } else {
                            String out = cr.getReturn();
                            screen(ANSI_GREEN + out + ANSI_RESET);
                        }
                    }
                } else {
                    screene( ANSI_RESET + "ERROR CODE : " + ANSI_RED + cr.getCode());
                    screene(ANSI_RESET + "   Command : " + ANSI_RED + cr.getCommand());
                    screene(ANSI_RESET + "     ERROR : " + ANSI_RED + cr.getError() + ANSI_RESET);
                }
            }
        }
    }

    private Completer initCompleters(CliEnvironment env) {

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
                + "hadoop-cli");
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
}
