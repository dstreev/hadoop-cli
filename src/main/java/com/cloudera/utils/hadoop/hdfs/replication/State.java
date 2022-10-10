
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

package com.cloudera.utils.hadoop.hdfs.replication;

import java.util.List;

public class State {

    private SyncStats current;
    private List<SyncStats> last10;


    public SyncStats getCurrent() {
        return current;
    }

    public void setCurrent(SyncStats current) {
        this.current = current;
    }

    public List<SyncStats> getLast10() {
        return last10;
    }

    public void setLast10(List<SyncStats> last10) {
        this.last10 = last10;
    }

    @Override
    public String toString() {
        return "State{" +
                "current=" + current +
                ", last10=" + last10 +
                '}';
    }
}
