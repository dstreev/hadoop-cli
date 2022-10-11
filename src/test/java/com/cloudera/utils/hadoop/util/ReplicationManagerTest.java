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

import com.cloudera.utils.hadoop.hdfs.util.ReplicationManager;
import org.junit.Test;

/**
 * Created by streever on 2016-04-06.
 */
public class ReplicationManagerTest {

    @Test
    public void optionTest001 () {
        String[] args = new String[]{"--file","/user/streever/replication_definitions/test001_def.json"};

        ReplicationManager rm = new ReplicationManager(args);

    }
}
