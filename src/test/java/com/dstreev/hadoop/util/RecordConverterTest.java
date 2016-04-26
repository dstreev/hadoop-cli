package com.dstreev.hadoop.util;

import com.dstreev.hadoop.mapreduce.parsers.JobCounterGroupParser;
import com.dstreev.hadoop.mapreduce.parsers.TaskCounterGroupParser;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by dstreev on 2016-04-25.
 */
public class RecordConverterTest {

    @Test
    public void testJobDetailParser001() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        InputStream resourceStream = classLoader.getResourceAsStream("job_detail.json");

        String resourceJson = IOUtils.toString(resourceStream);

        RecordConverter rc = new RecordConverter();

        Map<String, String> mapOut = rc.convert(null, resourceJson, "job", null);

        String header = rc.mapToRecord(mapOut, true, ",");

        System.out.println(header);

        String record = rc.mapToRecord(mapOut,false, ",");

        System.out.println(record);

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

        Map<String, String> mapOut = rc.convert(null, resourceJson, "jobCounters", tp);

        String header = rc.mapToRecord(mapOut, true, ",");

        System.out.println(header);

        String record = rc.mapToRecord(mapOut,false, ",");

        System.out.println(record);

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

        Map<String, String> mapOut = rc.convert(null, resourceJson, "jobTaskCounters", tp);

        String header = rc.mapToRecord(mapOut, true, ",");

        System.out.println(header);

        String record = rc.mapToRecord(mapOut,false, ",");

        System.out.println(record);
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

        Map<String, String> mapOut = rc.convert(null, resourceJson, "jobTaskAttemptCounters", tp);

        String header = rc.mapToRecord(mapOut, true, ",");

        System.out.println(header);

        String record = rc.mapToRecord(mapOut,false, ",");

        System.out.println(record);

    }

}
