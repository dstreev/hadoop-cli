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

package com.cloudera.utils.hadoop.shell.commands;

import java.util.Properties;

import com.cloudera.utils.hadoop.shell.command.AbstractCommand;
import com.cloudera.utils.hadoop.shell.Environment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class Env extends AbstractCommand {

    public Env(String name) {
        super(name);
    }

    @Override
    public String getDescription() {
        return "Review current 'env' variables";
    }

    public CommandReturn implementation(Environment env, CommandLine cmd, CommandReturn commandReturn) {
        if (cmd.hasOption("l") || cmd.getOptions().length == 0) {
            Properties props = env.getProperties();
            log(env, "Local Properties:");
            for (Object key : props.keySet()) {
                log(env, "\t" + key + "=" + props.get(key));
            }
            log(env, "System Properties:");
            props = System.getProperties();
            for (Object key : props.keySet()) {
                log(env, "\t" + key + "=" + props.get(key));
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
