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

package com.cloudera.utils.hadoop.hdfs.replication;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;


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
