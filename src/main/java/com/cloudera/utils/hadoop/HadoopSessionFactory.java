
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

package com.cloudera.utils.hadoop;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class HadoopSessionFactory extends BasePooledObjectFactory<HadoopSession> {
    @Override
    public HadoopSession create() throws Exception {
        String[] api = {"-api"};
        HadoopSession rtn = new HadoopSession();
        if (!rtn.start(api)) {
            throw new RuntimeException("Issue connecting to DFS.  Check Kerberos Auth and configs");
        }
        return rtn;
    }

    @Override
    public PooledObject<HadoopSession> wrap(HadoopSession hadoopSession) {
        return new DefaultPooledObject<HadoopSession>(hadoopSession);
    }
}
