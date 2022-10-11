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

package com.cloudera.utils.hadoop.util;

import com.cloudera.utils.hadoop.mapreduce.parsers.JobCounterGroupParser;
import com.cloudera.utils.hadoop.mapreduce.parsers.TaskCounterGroupParser;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by streever on 2016-04-25.
 */
public class RecordConverterTest {

    @Test
    public void testJobDetailParser001() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        InputStream resourceStream = classLoader.getResourceAsStream("job_detail.json");

        String resourceJson = IOUtils.toString(resourceStream);

        RecordConverter rc = new RecordConverter();

        Map<String, Object> mapOut = rc.convert(null, resourceJson, "job", null);

//        String header = rc.mapToRecord(mapOut, true, ",");

//        System.out.println(header);

//        String record = rc.mapToRecord(mapOut,false, ",");

//        System.out.println(record);

    }

    @Test
    public void testJobCountersParser001() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        InputStream resourceStream = classLoader.getResourceAsStream("job_counters.json");

        String resourceJson = IOUtils.toString(resourceStream);

        RecordConverter rc = new RecordConverter();

        TraversePath tp = new TraversePath();
        TraverseBehavior tbcg = new TraverseBehavior(TraverseBehavior.TRAVERSE_MODE.FLATTEN, new JobCounterGroupParser());
        tp.addPath("jobCounters.counterGroup", tbcg);

        Map<String, Object> mapOut = rc.convert(null, resourceJson, "jobCounters", tp);

//        String header = rc.mapToRecord(mapOut, true, ",");

//        System.out.println(header);

//        String record = rc.mapToRecord(mapOut,false, ",");

//        System.out.println(record);

    }

    @Test
    public void testTaskCountersParser001() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        InputStream resourceStream = classLoader.getResourceAsStream("task_counters.json");

        String resourceJson = IOUtils.toString(resourceStream);

        RecordConverter rc = new RecordConverter();

        TraversePath tp = new TraversePath();
        TraverseBehavior tbcg = new TraverseBehavior(TraverseBehavior.TRAVERSE_MODE.FLATTEN, new TaskCounterGroupParser());
        tp.addPath("jobTaskCounters.taskCounterGroup", tbcg);

        Map<String, Object> mapOut = rc.convert(null, resourceJson, "jobTaskCounters", tp);

//        String header = rc.mapToRecord(mapOut, true, ",");

//        System.out.println(header);

//        String record = rc.mapToRecord(mapOut,false, ",");

//        System.out.println(record);
    }


    @Test
    public void testTaskAttemptCountersParser001() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        InputStream resourceStream = classLoader.getResourceAsStream("task_attempt_counters.json");

        String resourceJson = IOUtils.toString(resourceStream);

        RecordConverter rc = new RecordConverter();

        TraversePath tp = new TraversePath();
        TraverseBehavior tbcg = new TraverseBehavior(TraverseBehavior.TRAVERSE_MODE.FLATTEN, new TaskCounterGroupParser());
        tp.addPath("jobTaskAttemptCounters.taskAttemptCounterGroup", tbcg);

        Map<String, Object> mapOut = rc.convert(null, resourceJson, "jobTaskAttemptCounters", tp);

//        String header = rc.mapToRecord(mapOut, true, ",");

//        System.out.println(header);

//        String record = rc.mapToRecord(mapOut,false, ",");

//        System.out.println(record);

    }

}
