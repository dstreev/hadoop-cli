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

import com.cloudera.utils.hadoop.hdfs.util.NamenodeJmxParser;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by streever on 2016-03-21.
 */
public class NamenodeParserTest {

    @Test
    public void getTopUserOpsString() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_active.json");
            List<Map<String,Object>> list = njp.getTopUserOpRecords();
            for (Map<String,Object> map: list) {
                Iterator<Map.Entry<String, Object>> iEntries = map.entrySet().iterator();
                while (iEntries.hasNext()) {
                    Map.Entry<String, Object> item = iEntries.next();
                    System.out.println(item.getKey() + ":" + item.getValue().toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getFSState() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_active.json");
            Map<String,Object> map = njp.getFSState();
            Iterator<Map.Entry<String, Object>> iEntries = map.entrySet().iterator();
            while (iEntries.hasNext()) {
                Map.Entry<String, Object> item = iEntries.next();
                System.out.println(item.getKey() + ":" + item.getValue().toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getNamenodeInfo() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_active.json");
            Map<String,Object> map = njp.getNamenodeInfo();
            Iterator<Map.Entry<String, Object>> iEntries = map.entrySet().iterator();
            while (iEntries.hasNext()) {
                Map.Entry<String, Object> item = iEntries.next();
                System.out.println(item.getKey() + ":" + item.getValue().toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getTopUserOpsStringStandby() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_standby.json");
            List<Map<String,Object>> list = njp.getTopUserOpRecords();
            for (Map<String,Object> map: list) {
                Iterator<Map.Entry<String, Object>> iEntries = map.entrySet().iterator();
                while (iEntries.hasNext()) {
                    Map.Entry<String, Object> item = iEntries.next();
                    System.out.println(item.getKey() + ":" + item.getValue().toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getFSStateStandby() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_standby.json");
            Map<String,Object> fsState = njp.getFSState();
//            System.out.println(fsState);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getNamenodeInfoStandby() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_standby.json");
            Map<String,Object> map = njp.getNamenodeInfo();
            Iterator<Map.Entry<String, Object>> iEntries = map.entrySet().iterator();
            while (iEntries.hasNext()) {
                Map.Entry<String, Object> item = iEntries.next();
                System.out.println(item.getKey() + ":" + item.getValue().toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getTopUserOpsStringStandalone() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.2.0_standalone.json");
            List<Map<String,Object>> list = njp.getTopUserOpRecords();
            for (Map<String,Object> map: list) {
                Iterator<Map.Entry<String, Object>> iEntries = map.entrySet().iterator();
                while (iEntries.hasNext()) {
                    Map.Entry<String, Object> item = iEntries.next();
                    System.out.println(item.getKey() + ":" + item.getValue().toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getFSStateStandalone() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.2.0_standalone.json");
            Map<String,Object> map = njp.getFSState();
            Iterator<Map.Entry<String, Object>> iEntries = map.entrySet().iterator();
            while (iEntries.hasNext()) {
                Map.Entry<String, Object> item = iEntries.next();
                System.out.println(item.getKey() + ":" + item.getValue().toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    @Test
    public void getNamenodeInfoStandalone() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.2.0_standalone.json");
            Map<String,Object> map = njp.getNamenodeInfo();
            Iterator<Map.Entry<String, Object>> iEntries = map.entrySet().iterator();
            while (iEntries.hasNext()) {
                Map.Entry<String, Object> item = iEntries.next();
                System.out.println(item.getKey() + ":" + item.getValue().toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

}
