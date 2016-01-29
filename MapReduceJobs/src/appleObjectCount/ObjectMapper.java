package appleObjectCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Data Sample
 *
 * BACKUP	583	1
 * BACKUP	594	1
 * CAL_ARCHIVES	568	1
 * CAL_ARCHIVES	558	1
 * CARD	570	1
 * CONTENT	617	1
 * CONTENT	619	1
 * BACKUP	587	1
 * CONTENT	647	1
 */

public class ObjectMapper extends Mapper <LongWritable, Text, StringPair, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String line = value.toString();
        String[] words = line.split("\\W+");

        if (words != null && words.length > 0 ){

            context.write(new StringPair(words[0]+"\t"+words[1], (words[2])), new Text(words[2]));
        }
    }
}