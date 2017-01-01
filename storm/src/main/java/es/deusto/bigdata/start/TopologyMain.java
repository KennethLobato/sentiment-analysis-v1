package es.deusto.bigdata.start;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import storm.kafka.bolt.KafkaBolt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import es.deusto.bigdata.topologies.TopologyFactory;
import es.deusto.bigdata.utils.StormRunner;

/**
 * Created by klobato on 11/12/2016.
 */

public class TopologyMain {

    private static final Logger LOG = LoggerFactory.getLogger(TopologyMain.class);
    private static final String TOPOLOGY_NAME = "tweet-ingestion";

    public static void main(String[] args) {

        try {

            boolean localTopology = true;
            if (args.length == 1 && args[0].equals("remote-topology")) {
                localTopology = false;
            }

            InputStream inputStream;
            inputStream = TopologyMain.class.getResourceAsStream("/local-defaults.properties");

            Properties props = new Properties();
            props.load(inputStream);

            Config conf = new Config();

            conf.put(Config.TOPOLOGY_WORKERS, Integer.parseInt(props.getProperty("Storm.TopologyWorkers", "1")));
            conf.put(Config.TOPOLOGY_DEBUG, Boolean.parseBoolean(props.getProperty("Storm.TopologyDebug", "false")));

            List<String> zkServers = new ArrayList<>(
                    Arrays.asList(props.getProperty("Zookeeper.Servers", "").split(","))
            );
            conf.put(Config.STORM_ZOOKEEPER_SERVERS, zkServers);
            conf.put(Config.STORM_ZOOKEEPER_PORT, Integer.parseInt(props.getProperty("Zookeeper.Port", "2181")));

            conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);

            StormTopology topology = TopologyFactory.getSentimentAnalysisTopology(props);
            if(localTopology) {
                StormRunner.runTopologyLocally(topology, TOPOLOGY_NAME, conf, Duration.ofSeconds(120000));
            } else {
                StormRunner.runTopologyRemotely(topology, TOPOLOGY_NAME, conf);
            }

        } catch (Exception ex) {
            LOG.error(ex.toString(), ex);
        }
    }
}
