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

package com.cloudera.utils.hadoop;

import com.cloudera.utils.hadoop.hdfs.shell.command.Direction;
import com.cloudera.utils.hadoop.cli.CliEnvironment;
import com.cloudera.utils.hadoop.shell.command.CommandReturn;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.math.NumberUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by streever on 2016-04-25.
 * <p>
 * Using the Resource Manager JMX, collect the stats on applications since the last time this was run or up to
 * 'n' (limit).
 */
@Slf4j
public abstract class AbstractQueryTimeFrameStats extends AbstractStats {
    protected Long increment = 60l * 60l * 1000l; // 1 hour

    /**
     * The earliest start time to get available jobs. Time since Epoch...
     */
    protected Long startTime = 0l;
    protected Long endTime = 0l;

    public AbstractQueryTimeFrameStats(String name) {
        super(name);
    }

    public AbstractQueryTimeFrameStats(String name, CliEnvironment env, Direction directionContext) {
        super(name, env, directionContext);
    }

    public AbstractQueryTimeFrameStats(String name, CliEnvironment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public AbstractQueryTimeFrameStats(String name, CliEnvironment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public AbstractQueryTimeFrameStats(String name, CliEnvironment env) {
        super(name, env);
    }

    @Override
    public CommandReturn processOptions(CliEnvironment cliEnvironment, CommandLine cmd, CommandReturn cr) {
        cr = super.processOptions(cliEnvironment, cmd, cr);

        try {

            Option[] cmdOpts = cmd.getOptions();
            String[] cmdArgs = cmd.getArgs();

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            // Default Behaviour
            // Set Start Time to previous day IF no config is specified.
            Calendar startCal = Calendar.getInstance();
            Date startDate = new Date(); // default today.
            if (cmd.hasOption("last")) {
                String lastOption = cmd.getOptionValue("last");
                String[] lastParts = lastOption.split("-");
                if (lastParts.length == 2 && NumberUtils.isCreatable(lastParts[0])) {
                    Integer window = Integer.parseInt(lastParts[0]);
                    if (lastParts[1].toUpperCase().startsWith("MIN")) {
                        startCal.add(Calendar.MINUTE, (-1 * window));
                        increment = 60l * 1000l;
                    } else if (lastParts[1].toUpperCase().startsWith("HOUR")) {
                        startCal.add(Calendar.HOUR, (-1 * window));
                        increment = 10l * 60l * 1000l; // ten minutes
                    } else if (lastParts[1].toUpperCase().startsWith("DAY")) {
                        startCal.add(Calendar.DAY_OF_MONTH, (-1 * window));
                        increment = 60l * 60l * 1000l; // 1 hour
                    } else {
                        // bad.
                        System.err.println("last option can't be parsed");
                        throw new RuntimeException("stat option 'l|last' can't be parsed");
                    }
                } else {
                    System.err.println("last option can't be parsed");
                    throw new RuntimeException("stat option 'l|last' can't be parsed");
                }
                startDate = startCal.getTime();
            } else if (cmd.hasOption("start")) {
                if (cmd.hasOption("start")) {
                    try {
                        startDate = df.parse(cmd.getOptionValue("start"));
                    } catch (ParseException e) {
                        log.error("Error parsing start date: {} - {} - {}", cmd.getOptionValue("start"), e.getMessage(), e.getClass().getName());
                        cr.setCode(CODE_BAD_DATE);
                        cr.getErr().print(e.getMessage());
                        return cr;
                    }
                }
            } else {
                // default is 1 day.
                startCal.add(Calendar.DAY_OF_MONTH, -1);
                startDate = startCal.getTime();
            }

            // TODO: Need to work in 'current'
            // Set the startTime
            startTime = startDate.getTime();

            if (cmd.hasOption("end")) {
                Date endDate = null;
                try {
                    endDate = df.parse(cmd.getOptionValue("end"));
                } catch (ParseException e) {
                    cr.setCode(CODE_BAD_DATE);
                    cr.getErr().print(e.getMessage());
                    return cr;
//                    e.printStackTrace();
//                    return new CommandReturn(CODE_BAD_DATE, e.getMessage()); // Bad Date
                }
                endTime = endDate.getTime();
            } else {
                // If no Config.
                // Set to now.
                endTime = new Date().getTime();
            }

            if (cmd.hasOption("increment")) {
                String incStr = cmd.getOptionValue("increment");
                increment = Long.parseLong(incStr) * 60l * 1000l;
            }
        } catch (Throwable t) {
            cr.setCode(CODE_STATS_ISSUE);
            cr.getErr().print(t.getMessage());
            return cr;
        }
        return cr;
    }

    public abstract void process(CommandLine cmdln);

    protected Map<String, String> getQueries(CommandLine cmd) {
        Map<String, String> rtn = new LinkedHashMap<String, String>();
        Long begin = startTime;
        Long end = endTime;

        if (begin + increment < end) {
            while (begin < end) {
                StringBuilder sb = new StringBuilder();
                StringBuilder sb2 = new StringBuilder();
                sb.append("finishedTimeBegin=").append(begin);
                sb2.append("finishedTimeBegin=").append(new Date(begin));
                begin = begin + increment - 1;
                sb.append("&finishedTimeEnd=").append(begin);
                sb2.append("&finishedTimeEnd=").append(new Date(begin));
                begin += 1;
                rtn.put(sb.toString(), sb2.toString());
            }
        }
        return rtn;
    }

    protected abstract void getHelp();

    @Override
    public Options getOptions() {
        Options opts = super.getOptions();

        OptionGroup beginOptionGroup = new OptionGroup();
        Option startOption = new Option("s", "start", true,
                "Start time for retrieval in 'yyyy-MM-dd HH:mm:ss'");
        startOption.setRequired(false);
        beginOptionGroup.addOption(startOption);

        Option lastOption = new Option("l", "last", true,
                "last x-DAY(S)|x-HOUR(S)|x-MIN(S). 1-HOUR=1 hour, 2-DAYS=2 days, 3-HOURS=3 hours, etc.");
        lastOption.setRequired(false);
        beginOptionGroup.addOption(lastOption);

        opts.addOptionGroup(beginOptionGroup);

        // TODO: WIP for current stats.
//        Option currentOption = new Option("c", "current", false, "Get Current / Active Records");
//        currentOption.setRequired(false);
//        beginOptionGroup.addOption(currentOption);

        Option endOption = new Option("e", "end", true,
                "End time for retrieval in 'yyyy-MM-dd HH:mm:ss'");
        endOption.setRequired(false);
        opts.addOption(endOption);

        Option incOption = new Option("inc", "increment", true, "Query Increment in minutes");
        incOption.setRequired(false);
        opts.addOption(incOption);

        return opts;
    }

}
