package com.dstreev.hadoop.hdfs.replication;

/**
 * Created by dstreev on 2016-04-05.
 */
public class Endpoint {

    /**
     * The hdfs uri for a NON HA Endpoint and HA Endpoint.  When HA is used, the name is the Service name.
     * For non-HA, it needs to be the host:port of the Namenode RPC endpoint.
     *
     */
    private String name;
    /**
     * If HA is enabled for the Endpoint, we need to look at the dfsNameService to determine
     * how to build it, if necessary, and reference it.
     */
    private Boolean haEnabled;
    /**
     * Support HA references.
     */
    private DfsNameService dfsNameService;
    /**
     * Directory for operation.
     */
    private String directory;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getHaEnabled() {
        return haEnabled;
    }

    public void setHaEnabled(Boolean haEnabled) {
        this.haEnabled = haEnabled;
    }

    public DfsNameService getDfsNameService() {
        return dfsNameService;
    }

    public void setDfsNameService(DfsNameService dfsNameService) {
        this.dfsNameService = dfsNameService;
    }

    public String getDirectory() {
        if (directory == null)
            throw new RuntimeException("Directory needs to be specified");
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    @Override
    public String toString() {
        return "Endpoint{" +
                "name='" + name + '\'' +
                ", haEnabled=" + haEnabled +
                ", dfsNameService=" + dfsNameService +
                ", directory='" + directory + '\'' +
                '}';
    }
}
