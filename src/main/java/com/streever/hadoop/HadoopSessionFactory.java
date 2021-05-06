package com.streever.hadoop;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class HadoopSessionFactory extends BasePooledObjectFactory<HadoopSession> {
    @Override
    public HadoopSession create() throws Exception {
        String[] api = {"-api"};
        HadoopSession rtn = new HadoopSession();
        rtn.start(api);
        return rtn;
    }

    @Override
    public PooledObject<HadoopSession> wrap(HadoopSession hadoopSession) {
        return new DefaultPooledObject<HadoopSession>(hadoopSession);
    }
}
