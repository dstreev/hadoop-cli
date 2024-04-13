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

/**
 * Created by streever on 2016-04-25.
 */
public class TraverseBehavior {
    public enum TRAVERSE_MODE {
        EXPLODE,
        FLATTEN
    }

    private final TRAVERSE_MODE mode;
    private final NodeParser nodeParser;

    public TraverseBehavior(TRAVERSE_MODE mode, NodeParser nodeParser) {
        this.mode = mode;
        this.nodeParser = nodeParser;
    }

    public TRAVERSE_MODE getMode() {
        return mode;
    }

    public NodeParser getNodeParser() {
        return nodeParser;
    }
}
