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
