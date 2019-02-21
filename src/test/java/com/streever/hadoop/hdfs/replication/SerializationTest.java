package com.streever.hadoop.hdfs.replication;

import org.junit.Test;

/**
 * Created by streever on 2016-04-06.
 */
public class SerializationTest {

    @Test
    public void DefinitionFromResourceTest001 () {
        Definition definition = ReplicationHelper.definitionFromResource("/default_def.json");

        System.out.println(definition.toString());

    }

    @Test
    public void StateFromStringTest001 () {
        State state = ReplicationHelper.stateFromResource("/default_state.json");

        System.out.println(state.toString());

    }

}
