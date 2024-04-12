
/*
 * Copyright (c) 2022-2024. David W. Streever All Rights Reserved
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

package com.cloudera.utils.hadoop.cli;

import org.apache.commons.pool2.ObjectPool;

public class HadoopSessionPool {

    private ObjectPool<HadoopSession> pool;

    public HadoopSessionPool(ObjectPool<HadoopSession> pool) {
        this.pool = pool;
    }

    public HadoopSession borrow()  {
        HadoopSession rtn = null;
        try {
            rtn = pool.borrowObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return rtn;
    }

    public void returnSession(HadoopSession session) {
        try {
            pool.returnObject(session);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        pool.close();
    }

}
