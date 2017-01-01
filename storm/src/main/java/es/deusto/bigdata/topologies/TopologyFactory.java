package es.deusto.bigdata.topologies;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import es.deusto.bigdata.spouts.TwitterSpout;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.selector.DefaultTopicSelector;

import es.deusto.bigdata.bolts.TweetFilterBolt;

import java.util.Properties;


/**
 * Created by klobato on 11/12/2016.
 */

public class TopologyFactory {

    public static StormTopology getSentimentAnalysisTopology(Properties props) {

        final String TWITTER_FILTER_CONSUMER_ID = "tweet-filter-bolt";
        final String TWITTER_SPOUT_ID = "twitter-spout";
        final String TWEETS_FILTERED_KAFKA_SINK_BOLT_ID = "tweet-filtered-to-kafka-bolt";

        String kafkaSink = props.getProperty("Kafka.Topic.TweetsFiltered");

        String ZkHosts = props.getProperty("Zookeeper.Servers");

        int twitterSpoutParallelism = Integer.parseInt(props.getProperty("Storm.TwitterSpout.Parallelism", "1"));
        int twitterSpoutNumberOfTasks = Integer.parseInt(props.getProperty("Storm.TwitterSpout.NumberOfTasks", "1"));

        int filterTweetsBoltParallelism = Integer.parseInt(props.getProperty("Storm.FilterTweets.Parallelism", "1"));
        int filterTweetsBoltNumberOfTasks = Integer.parseInt(props.getProperty("Storm.FilterTweets.NumberOfTasks", "1"));

        int kafkaSinkBoltParallelism = Integer.parseInt(props.getProperty("Storm.KafkaSinkBolt.Parallelism", "1"));
        int kafkaSinkBoltNumberOfTasks = Integer.parseInt(props.getProperty("Storm.KafkaSinkBolt.NumberOfTasks", "1"));

        TwitterSpout twitterSpout = new TwitterSpout();

        TweetFilterBolt filterBolt = new TweetFilterBolt(props);

        KafkaBolt kafkaSinkBolt = new KafkaBolt()
                .withTopicSelector(new DefaultTopicSelector(kafkaSink));

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TWITTER_SPOUT_ID, twitterSpout, twitterSpoutParallelism)
                .setNumTasks(twitterSpoutNumberOfTasks);

        builder.setBolt(TWITTER_FILTER_CONSUMER_ID, filterBolt, filterTweetsBoltParallelism)
                .setNumTasks(filterTweetsBoltNumberOfTasks)
                .shuffleGrouping(TWITTER_SPOUT_ID);

        builder.setBolt(TWEETS_FILTERED_KAFKA_SINK_BOLT_ID, kafkaSinkBolt, kafkaSinkBoltParallelism)
                .setNumTasks(kafkaSinkBoltNumberOfTasks)
                .shuffleGrouping(TWITTER_FILTER_CONSUMER_ID);

        return builder.createTopology();
    }
}
