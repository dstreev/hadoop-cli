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

package com.cloudera.utils.hadoop.yarn.parsers;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by streever on 2016-04-26.
 */
public class QueueParserTest {

    private boolean header = false;

    public boolean isHeader() {
        return header;
    }

    public void setHeader(boolean header) {
        this.header = header;
    }

    //    @Test
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
