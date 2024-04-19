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

package com.cloudera.utils.hadoop.yarn.parsers;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class QueueParserTest {

    private boolean header = false;

    public boolean isHeader() {
        return header;
    }

    public void setHeader(boolean header) {
        this.header = header;
    }

    @Test
    public void queueTest001() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        InputStream resourceStream = classLoader.getResourceAsStream("scheduler.json");

        String schedulerJson = IOUtils.toString(resourceStream);

        QueueParser queueParser = new QueueParser(schedulerJson);

        Map<String, List<Map<String, Object>>> queueStatMap = queueParser.getQueues("now");


        Iterator<Map.Entry<String, List<Map<String, Object>>>> rIter = queueStatMap.entrySet().iterator();
        while (rIter.hasNext()) {
            Map.Entry<String, List<Map<String, Object>>> recordSet = rIter.next();
            print(recordSet.getKey(), recordSet.getValue());
        }
        System.out.println("Hello");
    }

    @Test
    public void queueTest002() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();

        InputStream resourceStream = classLoader.getResourceAsStream("scheduler_large.json");

        String schedulerJson = IOUtils.toString(resourceStream);

        QueueParser queueParser = new QueueParser(schedulerJson);

        Map<String, List<Map<String, Object>>> queueStatMap = queueParser.getQueues("now");

        setHeader(true);

        Iterator<Map.Entry<String, List<Map<String, Object>>>> rIter = queueStatMap.entrySet().iterator();
        while (rIter.hasNext()) {
            Map.Entry<String, List<Map<String, Object>>> recordSet = rIter.next();
            print(recordSet.getKey(), recordSet.getValue());
        }

        setHeader(false);

        Iterator<Map.Entry<String, List<Map<String, Object>>>> rIter2 = queueStatMap.entrySet().iterator();
        while (rIter2.hasNext()) {
            Map.Entry<String, List<Map<String, Object>>> recordSet = rIter2.next();
            print(recordSet.getKey(), recordSet.getValue());
        }

        System.out.println("Hello");
    }

    protected void print(String recordSet, List<Map<String, Object>> records) {
        System.out.println("Record set: " + recordSet);
        int i = 0;
        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> record: records) {
            i++;
            if (i % 8000 == 0)
                System.out.println(".");
            else if (i % 100 == 0)
                System.out.print(".");

            // TODO: Fix
//            String recordStr = RecordConverter.mapToRecord(record, header, ",");
//            if (header) {
//                System.out.println(recordStr);
//                // Terminate Loop.
//                break;
//            } else {
//                sb.append(recordStr).append("\n");
//            }
        }
        // If the options say to write to hdfs.
//        if (baseOutputDir != null) {
//            String outputFilename = dfFile.format(new Date()) + ".txt";
//            HdfsWriter writer = new HdfsWriter(fs, baseOutputDir + "/" + recordSet + "/" + outputFilename);
//            writer.append(sb.toString().getBytes());
//        } else {
            System.out.println(sb.toString());
//        }

    }

}
