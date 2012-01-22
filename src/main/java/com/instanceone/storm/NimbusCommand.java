// Copyright (c) 2012 Health Market Science, Inc.

package com.instanceone.storm;


public class NimbusCommand {
/*
    public static void main(String[] args) throws Exception{
        NimbusClient nimbusClient = new NimbusClient("dlcirrus01");
        Client client =  nimbusClient.getClient();
        ClusterSummary summary = client.getClusterInfo();
        
        List<TopologySummary> topos = summary.get_topologies();
        for(TopologySummary ts : topos){
            System.out.println(ts.get_name() + " (" + ts.get_id() + ")");
            System.out.println("Tasks/Workers: " + ts.get_num_tasks() + "/" + ts.get_num_workers());
            System.out.println("Status: " + ts.get_status());
            System.out.println("Uptime: " + ts.get_uptime_secs());
            
            TopologyInfo info = client.getTopologyInfo(ts.get_id());
            List<TaskSummary> tasks = info.get_tasks();
            for(TaskSummary task : tasks){
                System.out.println("\t" + task.get_component_id());
//                task.
            }
            
        }
        
        
    }
    */
}
