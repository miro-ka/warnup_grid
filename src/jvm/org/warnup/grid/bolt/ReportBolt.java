package org.warnup.grid.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.warnup.grid.tools.DBWriter;

import java.util.Map;


/**
* A bolt that prints the word and count to DBWriter (currently sqlite)
*/
public class ReportBolt extends BaseRichBolt
{
    DBWriter dbWriter;

    @Override
    public void prepare(
            Map map,
            TopologyContext         topologyContext,
            OutputCollector         outputCollector)
    {
        try {
            dbWriter = new DBWriter();
        }catch (Exception e) {
            System.out.println(e.toString());
        }

    }

    @Override
    public void execute(Tuple tuple)
    {
        // access the first column 'word'
        final Long tweet_id = tuple.getLongByField("tweet_id");
        final Integer count = tuple.getIntegerByField("count");
        dbWriter.insert(tweet_id, count);
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
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // nothing to add - since it is the final bolt
    }
}
