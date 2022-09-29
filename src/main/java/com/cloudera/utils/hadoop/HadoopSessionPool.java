package com.cloudera.utils.hadoop;

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
