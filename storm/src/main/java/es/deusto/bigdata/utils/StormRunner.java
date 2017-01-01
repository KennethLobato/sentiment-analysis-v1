package es.deusto.bigdata.utils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;

import java.time.Duration;


/**
 * Created by klobato on 11/12/2016.
 */


public class StormRunner {
    public static void runTopologyLocally(StormTopology topology, String topologyName, Config conf, Duration runtime) throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, topology);
        Thread.sleep(runtime.toMillis());
        cluster.killTopology(topologyName);
        cluster.shutdown();
    }

    public static void runTopologyRemotely(StormTopology topology, String topologyName, Config conf) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        StormSubmitter.submitTopology(topologyName, conf, topology);
    }
}
