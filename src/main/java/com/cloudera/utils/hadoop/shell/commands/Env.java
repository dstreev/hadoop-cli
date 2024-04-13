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

package com.cloudera.utils.hadoop.shell.commands;

import java.util.Properties;

import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

@Slf4j
public class Env extends AbstractCommand {

    public Env(String name) {
        super(name);
    }

    @Override
    public String getDescription() {
        return "Review current 'env' variables";
    }

    public CommandReturn implementation(CliEnvironment env, CommandLine cmd, CommandReturn commandReturn) {
        if (cmd.hasOption("l") || cmd.getOptions().length == 0) {
            Properties props = env.getProperties();
            log.debug("Local Properties:");
            for (Object key : props.keySet()) {
                log.debug("\t{}={}", key, props.get(key));
            }
            log.debug("System Properties:");
            props = System.getProperties();
            for (Object key : props.keySet()) {
                log.debug("\t{}={}", key, props.get(key));
            }
        }
        if (cmd.hasOption("s")) {
            String input = cmd.getOptionValue("s");
            String inputParts[] = input.split("=");
            if (inputParts.length == 2) {
                env.getProperties().setProperty(inputParts[0], inputParts[1]);
            }
        }
        if (cmd.hasOption("u")) {
            String input = cmd.getOptionValue("u");
            env.getProperties().remove(input);
        }

        return commandReturn;
    }

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();
//        opts.addOption("s", "system", false, "list system properties.");
//        opts.addOption("l", "local", false, "list local properties.");

        Option setOption = new Option("s", "set", true,
                "set and environment var is the form of var=value");
        setOption.setRequired(false);
        opts.addOption(setOption);

        Option unsetOption = new Option("u", "unset", true, "unset an environment var.");
        unsetOption.setRequired(false);
        opts.addOption(unsetOption);

        Option listOption = new Option("l", "list", false, "Show Environment Variables");
        listOption.setRequired(false);
        opts.addOption(listOption);

        return opts;
    }

}
