package org.warnup.grid.bolt;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.warnup.grid.tools.DBWriter;

import java.util.Map;


/**
* A bolt that prints the word and count to redis and DBWriter (currently sqlite)
*/
public class ReportBolt extends BaseRichBolt
{
    // place holder to keep the connection to redis
    //transient RedisConnection<String,String> redis;
    DBWriter db_writer;

    @Override
    public void prepare(
            Map map,
            TopologyContext         topologyContext,
            OutputCollector         outputCollector)
    {
        // instantiate a redis connection
        //RedisClient client = new RedisClient("localhost",6379);
        db_writer = new DBWriter();

        // initiate the actual connection
       // redis = client.connect();
    }

    @Override
    public void execute(Tuple tuple)
    {
        // access the first column 'word'
        Long tweet_id = tuple.getLongByField("tweet_id");
        Integer count = tuple.getIntegerByField("count");
        db_writer.insert(tweet_id, count);
        //String word = tuple.getStringByField("tweet_text");

        //write to DB
        /*
        "tweet_id",
        "fav_count",
        "retweet_count",
        "tweet_text"));
        */

        // access the second column 'count'

        //String word = tuple.getStringByField("tweet");
        //Integer count = 20;

        // publish the word count to redis using word as the key
        //redis.publish("WordCountTopology", Long.toString(tweet_id) + "|" + Integer.toString(count));
        //redis.publish("WordCountTopology", word );
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // nothing to add - since it is the final bolt
    }
}
