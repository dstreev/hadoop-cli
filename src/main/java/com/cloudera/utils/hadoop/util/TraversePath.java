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

import java.util.Map;
import java.util.TreeMap;

/**
 * Created by streever on 2016-04-25.
 */
public class TraversePath {

    private Map<String, TraverseBehavior> paths = new TreeMap<String, TraverseBehavior>();

    public TraversePath() {

    }

    public void addPath(String pathKey, TraverseBehavior path) {
        paths.put(pathKey, path);
    }

    public Map<String, TraverseBehavior> getPaths() {
        return paths;
    }

    public void clear() {
        paths = new TreeMap<String, TraverseBehavior>();
    }

}
