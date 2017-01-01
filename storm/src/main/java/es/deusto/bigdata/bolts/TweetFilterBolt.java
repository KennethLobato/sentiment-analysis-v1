package es.deusto.bigdata.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import twitter4j.json.DataObjectFactory;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;


/**
 * Created by klobato on 11/12/2016.
 */

public class TweetFilterBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(TweetFilterBolt.class);
    private static final String MESSAGE_FIELD_RAW = "tweetRAW";
    private static final String KEY_FIELD = "key";
    private static final String MESSAGE_FIELD = "message";

    private OutputCollector collector;

    public TweetFilterBolt(Properties props)
    {
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector){
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Status tweet = (Status)tuple.getValueByField(MESSAGE_FIELD_RAW);

            if(tweet.getLang() != null &&
                    tweet.getLang().equals("en") &&
                    tweet.getText() != null) {
                Gson gson = new Gson();
                String json = gson.toJson(tweet);
                System.out.println(tweet.getText());
                collector.emit(new Values(tweet.getText().getBytes(), json.getBytes()));
            }
            collector.ack(tuple);
        } catch (Exception ex) {
            LOG.error(ex.toString(), ex);
            collector.reportError(ex);
            throw new FailedException(ex);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(KEY_FIELD, MESSAGE_FIELD));
    }

    @Override
    public void cleanup() {
    }
}

