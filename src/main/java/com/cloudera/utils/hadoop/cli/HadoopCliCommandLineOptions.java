/*
 * Copyright (c) 2024. David W. Streever All Rights Reserved
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

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.util.Strings;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
@Order(1)
@Slf4j
@Getter
@Setter
public class HadoopCliCommandLineOptions {

    public static String SPRING_CONFIG_PREFIX = "hadoop.cli";

    public static void main(String[] args) {
        HadoopCliCommandLineOptions pcli = new HadoopCliCommandLineOptions();
        String[] convertedArgs = pcli.toSpringBootOption(args);
        String newCmdLn = String.join(" ", convertedArgs);
        System.out.println(newCmdLn);
    }

    public CommandLine getCommandLine(String[] args) {
        Options options = getOptions();

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException pe) {
            System.out.println("Missing Arguments: " + pe.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            String cmdline = Shell.substituteVariablesFromManifest("hadoopcli <options> \nversion:${HadoopCLI-Version}");
            formatter.printHelp(100, cmdline, "Hadoop CLI Utility", options,
                    "\nVisit https://github.com/dstreev/hadoop-cli/blob/main/README.md for detailed docs");
            System.exit(-1);

        }

        if (cmd.hasOption("h")) {
            HelpFormatter formatter = new HelpFormatter();
            String cmdline = Shell.substituteVariablesFromManifest("hadoopcli <options> \nversion:${HadoopCLI-Version}");
            formatter.printHelp(100, cmdline, "Hadoop CLI Utility", options,
                    "\nVisit https://github.com/dstreev/hadoop-cli/blob/main/README.md for detailed docs");
            System.exit(0);

        }

        return cmd;
    }

    private Options getOptions() {
        // create Options object
        Options options = new Options();

        // add i option
        Option initOption = new Option("i", "init", true, "Initialization with Set");
        initOption.setRequired(false);
        // Commons-Cli v1.3+ (can't use currently because of Hadoop Commons-cli version is at 1.2.
        //        Option initOption = Option.builder("i").required(false)
        //                .argName("init set").desc("Initialize with set")
        //                .longOpt("init")
        //                .hasArg(true).numberOfArgs(1)
        //                .build();
        options.addOption(initOption);

        Option executeOption = new Option("e", "execute", true, "Execute Command");
        executeOption.setRequired(false);
        //        Option executeOption = Option.builder("e").required(false)
        //                .argName("command [args]").desc("Execute Command")
        //                .longOpt("execute")
        //                .hasArg(true).numberOfArgs(1)
        //                .build();
        options.addOption(executeOption);

        // add f option
        Option fileOption = new Option("f", "file", true, "File to execute");
        fileOption.setRequired(false);
        //        Option fileOption = Option.builder("f").required(false)
        //                .argName("file to exec").desc("Run File and Exit")
        //                .longOpt("file")
        //                .hasArg(true).numberOfArgs(1)
        //                .build();
        options.addOption(fileOption);

        Option templateOption = new Option("t", "template", true,
                "Template to apply on input (-f | -stdin)");
        templateOption.setRequired(false);
        //        Option templateOption = Option.builder("t").required(false)
        //                .argName("template").desc("Template to apply on input (-f | -stdin)")
        //                .longOpt("template")
        //                .hasArg(true).numberOfArgs(1)
        //                .build();
        options.addOption(templateOption);

        Option delimiterOption = new Option("td", "template-delimiter", true,
                "Delimiter to apply to 'input' for template option (default=',')");
        delimiterOption.setRequired(false);
        //        Option delimiterOption = Option.builder("td").required(false)
        //                .argName("template-delimiter").desc("Delimiter to apply to 'input' for template option (default=',')")
        //                .longOpt("template-delimiter")
        //                .hasArg(true).numberOfArgs(1)
        //                .build();
        options.addOption(delimiterOption);

        // add stdin option
        Option siOption = new Option("stdin", "stdin", false, "Run Stdin pipe and Exit");
        siOption.setRequired(false);
        //        Option siOption = Option.builder("stdin").required(false)
        //                .argName("stdin process").desc("Run Stdin pipe and Exit")
        //                .longOpt("stdin")
        //                .hasArg(false)
        //                .build();
        options.addOption(siOption);

        Option silentOption = new Option("s", "silent", false, "Suppress Banner");
        silentOption.setRequired(false);
        //        Option silentOption = Option.builder("s").required(false)
        //                .argName("silent").desc("Suppress Banner")
        //                .longOpt("silent")
        //                .hasArg(false)
        //                .build();
        options.addOption(silentOption);

        Option apiOption = new Option("api", "api", false, "API mode");
        apiOption.setRequired(false);
        //        Option apiOption = Option.builder("api").required(false)
        //                .argName("api").desc("API mode")
        //                .longOpt("api")
        //                .hasArg(false)
        //                .build();
        options.addOption(apiOption);

        Option verboseOption = new Option("v", "verbose", false, "Verbose Commands");
        verboseOption.setRequired(false);
        //        Option verboseOption = Option.builder("v").required(false)
        //                .argName("verbose").desc("Verbose Commands")
        //                .longOpt("verbose")
        //                .hasArg(false)
        //                .build();
        options.addOption(verboseOption);

        Option debugOption = new Option("d", "debug", false, "Debug Commands");
        debugOption.setRequired(false);
        //        Option debugOption = Option.builder("d").required(false)
        //                .argName("debug").desc("Debug Commands")
        //                .longOpt("debug")
        //                .hasArg(false)
        //                .build();
        options.addOption(debugOption);

        Option envOption = new Option("ef", "env-file", true, "Environment File(java properties format) with a list of key=values");
        envOption.setRequired(false);
        //        Option debugOption = Option.builder("d").required(false)
        //                .argName("debug").desc("Debug Commands")
        //                .longOpt("debug")
        //                .hasArg(false)
        //                .build();
        options.addOption(envOption);


        Option helpOption = new Option("h", "help", false, "Help");
        helpOption.setRequired(false);
        //        Option helpOption = Option.builder("h").required(false)
        //                .longOpt("help")
        //                .build();
        options.addOption(helpOption);

        // TODO: Scripting
        //options.addOption("f", true, "Script file");

        return options;
    }

    public String[] toSpringBootOption(String[] args) {
        CommandLine cmd = getCommandLine(args);
        List<String> springOptions = new ArrayList<>();
        for (Option option : cmd.getOptions()) {
            String opt = option.getLongOpt();
            String[] values = option.getValues();
            if (values != null && values.length > 0) {
                springOptions.add("--" + SPRING_CONFIG_PREFIX + "." + opt + "=" + String.join(",", values));
            } else {
                springOptions.add("--" + SPRING_CONFIG_PREFIX + "." + opt + "=" + "true");
            }
        }
        // Collect Legacy Command Line Options and pass them to the Spring Boot Application as a single string.
        String clo = Strings.join(Arrays.asList(args), ' ');
        springOptions.add("--" + SPRING_CONFIG_PREFIX + ".legacy-command-line-options=" + clo + "\"");
        return springOptions.toArray(new String[0]);
    }

}
