
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

import com.cloudera.utils.hadoop.hdfs.util.NamenodeJmxParser;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by streever on 2016-03-21.
 */
@Slf4j
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
            log.error("Error parsing Jmx JSON", e);
            assertTrue(false);
        }
    }

    @Test
    public void getFSState() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_active.json");
            Map<String,Object> map = njp.getFSState();
            for (Map.Entry<String, Object> item : map.entrySet()) {
                log.info("{}:{}", item.getKey(), item.getValue().toString());
            }
        } catch (Exception e) {
            log.error("Error parsing Jmx JSON", e);
            fail();
        }
    }

    @Test
    public void getNamenodeInfo() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_active.json");
            Map<String,Object> map = njp.getNamenodeInfo();
            for (Map.Entry<String, Object> item : map.entrySet()) {
                log.info("{}:{}", item.getKey(), item.getValue().toString());
            }
        } catch (Exception e) {
            log.error("Error parsing Jmx JSON", e);
            fail();
        }
    }

    @Test
    public void getTopUserOpsStringStandby() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_standby.json");
            List<Map<String,Object>> list = njp.getTopUserOpRecords();
            for (Map<String,Object> map: list) {
                for (Map.Entry<String, Object> item : map.entrySet()) {
                    log.info("{}:{}", item.getKey(), item.getValue().toString());
                }
            }
        } catch (Exception e) {
            log.error("Error parsing Jmx JSON", e);
            fail();
        }
    }

    @Test
    public void getFSStateStandby() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_standby.json");
            Map<String,Object> fsState = njp.getFSState();
//            System.out.println(fsState);
        } catch (Exception e) {
            log.error("Error parsing Jmx JSON", e);
            fail();
        }
    }

    @Test
    public void getNamenodeInfoStandby() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.5.1_standby.json");
            Map<String,Object> map = njp.getNamenodeInfo();
            for (Map.Entry<String, Object> item : map.entrySet()) {
                log.info("{}:{}", item.getKey(), item.getValue().toString());
            }
        } catch (Exception e) {
            log.error("Error parsing Jmx JSON", e);
            fail();
        }
    }

    @Test
    public void getTopUserOpsStringStandalone() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.2.0_standalone.json");
            List<Map<String,Object>> list = njp.getTopUserOpRecords();
            for (Map<String,Object> map: list) {
                for (Map.Entry<String, Object> item : map.entrySet()) {
                    log.info("{}:{}", item.getKey(), item.getValue().toString());
                }
            }
        } catch (Exception e) {
            log.error("Error parsing Jmx JSON", e);
            fail();
        }
    }

    @Test
    public void getFSStateStandalone() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.2.0_standalone.json");
            Map<String,Object> map = njp.getFSState();
            for (Map.Entry<String, Object> item : map.entrySet()) {
                log.info("{}:{}", item.getKey(), item.getValue().toString());
            }
        } catch (Exception e) {
            log.error("Error parsing Jmx JSON", e);
            fail();
        }
    }

    @Test
    public void getNamenodeInfoStandalone() {
        try {
            NamenodeJmxParser njp = new NamenodeJmxParser("nn_2.3.2.0_standalone.json");
            Map<String,Object> map = njp.getNamenodeInfo();
            for (Map.Entry<String, Object> item : map.entrySet()) {
                log.info("{}:{}", item.getKey(), item.getValue().toString());
            }
        } catch (Exception e) {
            log.error("Error parsing Jmx JSON", e);
            fail();
        }
    }

}
