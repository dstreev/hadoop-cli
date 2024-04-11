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

package com.cloudera.utils.hadoop.yarn;

import com.cloudera.utils.hadoop.AbstractStats;
import com.cloudera.utils.hadoop.hdfs.shell.command.Direction;
import com.cloudera.utils.hadoop.cli.CliEnvironment;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import java.util.*;

/**
 * Created by streever on 2016-04-25.
 * <p>
 * Using the Resource Manager JMX, collect the queue stats .
 */
public class SchedulerStatsCommand extends AbstractStats {

    private SchedulerStats schedulerStats = null;
    public static final String QUEUE = "queue";

    private String timestamp = null;

    public SchedulerStatsCommand(String name) {
        super(name);
    }

    @Override
    public String getDescription() {
        return "Collect Queue Stats from the YARN REST API";
    }

    public SchedulerStatsCommand(String name, CliEnvironment env, Direction directionContext) {
        super(name, env, directionContext);
//        schedulerStats = new SchedulerStatsImpl(env.getConfig());
    }

    public SchedulerStatsCommand(String name, CliEnvironment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public SchedulerStatsCommand(String name, CliEnvironment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public SchedulerStatsCommand(String name, CliEnvironment env) {
        super(name, env);
    }

    protected SchedulerStats getSchedulerStats() {
        if (schedulerStats == null)
            schedulerStats = new SchedulerStats(env.getConfig());
        return schedulerStats;
    }

    @Override
    public void process(CommandLine cmdln) {
        getSchedulerStats().process(cmdln);
        Iterator<Map.Entry<String, List<Map<String, Object>>>> rIter = getSchedulerStats().getRecords().entrySet().iterator();
        while (rIter.hasNext()) {
            Map.Entry<String, List<Map<String, Object>>> recordSet = rIter.next();
            print(recordSet.getKey(), getSchedulerStats().getRecordFieldMap().get(recordSet.getKey()), recordSet.getValue());
        }
        getSchedulerStats().clearCache();
    }

    @Override
    public Options getOptions() {
        Options options = super.getOptions();
        Options csOptions = getSchedulerStats().getOptions();
        for (Object option: csOptions.getOptions()) {
            if (option instanceof Option) {
                options.addOption((Option)option);
            } else if (option instanceof OptionGroup) {
                options.addOptionGroup((OptionGroup)option);
            }
        }
        return options;
    }

    protected void getHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append("Collect Queue Stats from the YARN REST API.").append("\n");

        System.out.println(sb.toString());
    }


}
