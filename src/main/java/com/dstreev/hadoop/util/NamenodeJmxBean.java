package com.dstreev.hadoop.util;

/**
 * Created by dstreev on 2016-03-24.
 */
public enum NamenodeJmxBean {
    FS_STATE_JMX_BEAN("Hadoop:service=NameNode,name=FSNamesystemState"),
    NN_INFO_JMX_BEAN("Hadoop:service=NameNode,name=NameNodeInfo");

    private String beanName;

    public String getBeanName() {
        return beanName;
    }

    NamenodeJmxBean(String beanName) {
        this.beanName = beanName;
    }

}
