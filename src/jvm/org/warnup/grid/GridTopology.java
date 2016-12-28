package org.warnup.grid;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.warnup.grid.bolt.CountBolt;
import org.warnup.grid.bolt.ParseTweetBolt;
import org.warnup.grid.bolt.ReportBolt;
import org.warnup.grid.spout.twitter.TwitterSpout;

import java.util.Properties;


public class GridTopology {

    public static void main(String[] args) throws Exception {


        TopologyBuilder builder = new TopologyBuilder();

        // Load config properties
        Properties properties = new Properties();
        properties.load(GridTopology.class.getResourceAsStream("/config.properties"));
        final String key = properties.getProperty("twitter.key");
        final String secret = properties.getProperty("twitter.secret");
        final String token = properties.getProperty("twitter.token");
        final String tokensecret = properties.getProperty("twitter.tokensecret");


        final TwitterSpout tweetSpout = new TwitterSpout(key, secret, token, tokensecret);


        // attach the tweet spout to the topology - parallelism of 1
        builder.setSpout("tweet-spout", tweetSpout, 1);
        builder.setBolt("split-tweet-bolt", new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout");
        builder.setBolt("count-bolt", new CountBolt(), 15).fieldsGrouping("split-tweet-bolt", new Fields("tweet_id"));
        builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("count-bolt");



        Config conf = new Config();
        conf.setDebug(false);


        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("warnup-grid", conf, builder.createTopology());
            //Utils.sleep(10000);
            //cluster.killTopology("warnup-grid");
            //cluster.shutdown();
        }
    }
}
