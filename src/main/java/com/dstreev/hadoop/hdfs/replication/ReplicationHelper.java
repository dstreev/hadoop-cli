package com.dstreev.hadoop.hdfs.replication;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * Created by dstreev on 2016-04-06.
 */
public class ReplicationHelper {

    private static ObjectMapper mapper = new ObjectMapper();

    public static Definition definitionFromString(String source) {
        Definition definition = null;

        try {
            definition = mapper.readValue(source, Definition.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return definition;
    }

    public static Definition definitionFromResource(String location) {
        Definition definition = null;

        try {
            URL url = ReplicationHelper.class.getResource(location);
            definition = mapper.readValue(new File(url.toURI()), Definition.class);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        return definition;
    }

    public static Definition definitionFromInputStream(InputStream in) {
        Definition definition = null;

        try {
            definition = mapper.readValue(in, Definition.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return definition;
    }

    public static State stateFromString(String source) {
        State state = null;

        try {
            state = mapper.readValue(source, State.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return state;
    }

    public static State stateFromResource(String location) {
        State state = null;

        try {
            URL url = ReplicationHelper.class.getResource(location);
            state = mapper.readValue(new File(url.toURI()), State.class);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        return state;
    }

    public static State stateFromInputStream(InputStream in) {
        State state = null;

        try {
            state = mapper.readValue(in, State.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return state;
    }

    public static String getSourceDirectory(Definition definition, Configuration config) {
        String prefix = config.get("fs.defaultFS");
        String sourceDir = definition.getSource().getDirectory();
        return prefix + sourceDir;
    }

    public static String getTargetDirectory(Definition definition) {
        StringBuilder sb = new StringBuilder();
        sb.append("hdfs://");
        // The 'name' will be either the Service Name (HA) or the host:port for non-HA.
        sb.append(definition.getTarget().getName());
        return sb.toString();
    }
}
