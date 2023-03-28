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
import com.cloudera.utils.hadoop.shell.Environment;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by streever on 2016-04-25.
 * <p>
 * Using the Resource Manager JMX, collect the stats on applications since the last time this was run or up to
 * 'n' (limit).
 */
public class ContainerStatsCommand extends AbstractStats {

    private ContainerStats containerStats = null;
    @Override
    public String getDescription() {
        return "Collect Container Stats from the YARN REST API";
    }

    public ContainerStatsCommand(String name, Environment env, Direction directionContext) {
        super(name, env, directionContext);
//        appStats = new ContainerStatsImpl(env.getConfig());
    }

    public ContainerStatsCommand(String name, Environment env, Direction directionContext, int directives) {
        super(name, env, directionContext, directives);
    }

    public ContainerStatsCommand(String name, Environment env, Direction directionContext, int directives, boolean directivesBefore, boolean directivesOptional) {
        super(name, env, directionContext, directives, directivesBefore, directivesOptional);
    }

    public ContainerStatsCommand(String name, Environment env) {
        super(name, env);
    }

    protected ContainerStats getContainerStats() {
        if (containerStats == null)
            containerStats = new ContainerStats(env.getConfig());
        return containerStats;
    }
    @Override
    public void process(CommandLine cmdln) {
        getContainerStats().process(cmdln);
        Iterator<Map.Entry<String, List<Map<String, Object>>>> rIter = getContainerStats().getRecords().entrySet().iterator();
        while (rIter.hasNext()) {
            Map.Entry<String, List<Map<String, Object>>> recordSet = rIter.next();
            print(recordSet.getKey(), getContainerStats().getRecordFieldMap().get(recordSet.getKey()), recordSet.getValue());
        }
        getContainerStats().clearCache();
    }

    @Override
    public Options getOptions() {
        Options options = super.getOptions();
        Options csOptions = getContainerStats().getOptions();
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
        sb.append("Collect Container Stats for the YARN REST API.").append("\n");

        System.out.println(sb.toString());
    }


}
