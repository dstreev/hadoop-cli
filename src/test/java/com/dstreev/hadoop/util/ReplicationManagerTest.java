package com.dstreev.hadoop.util;

import com.dstreev.hadoop.hdfs.util.ReplicationManager;
import org.junit.Test;

/**
 * Created by dstreev on 2016-04-06.
 */
public class ReplicationManagerTest {

    @Test
    public void optionTest001 () {
        String[] args = new String[]{"--file","/user/dstreev/replication_definitions/test001_def.json"};

        ReplicationManager rm = new ReplicationManager(args);

    }
}
