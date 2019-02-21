package com.streever.hadoop.util;

import com.streever.hadoop.hdfs.util.ReplicationManager;
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
