package org.warnup.grid.spout;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;



public class TwitterSpout extends BaseRichSpout {

    public static Logger log = LoggerFactory.getLogger(TwitterSpout.class);
    private Credentials credentials;

    boolean _isDistributed;
    SpoutOutputCollector _collector;

    public TwitterSpout(final Credentials credentials) {
        this(true, credentials);
        this.credentials = credentials;
    }


    public TwitterSpout(final boolean isDistributed, final Credentials credentials) {
        this._isDistributed = isDistributed;
        this.credentials = credentials;
    }



    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;
    }


    public void close() {
    }


    public void nextTuple() {
        Utils.sleep(100L);
        String[] words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
        Random rand = new Random();
        String word = words[rand.nextInt(words.length)];
        this._collector.emit(new Values(new Object[]{word}));
    }


    public void ack(Object msgId) {
    }


    public void fail(Object msgId) {
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(new String[]{"word"}));
    }


    public Map<String, Object> getComponentConfiguration() {
        if(!this._isDistributed) {
            HashMap ret = new HashMap();
            ret.put("topology.max.task.parallelism", Integer.valueOf(1));
            return ret;
        } else {
            return null;
        }
    }
}
