package com.dstreev.hadoop.yarn.parsers;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by dstreev on 2016-04-26.
 */
public class QueueParserTest {

    @Test
    public void queueTest001() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        InputStream resourceStream = classLoader.getResourceAsStream("scheduler.json");

        String schedulerJson = IOUtils.toString(resourceStream);

        QueueParser queueParser = new QueueParser(schedulerJson);

        Map<String,List<Map<String,String>>> queueList = queueParser.getQueues();

        System.out.println("Hello");
    }
}
