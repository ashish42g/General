package mmscJob;

import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Class Name: MMCSMRMigrationReducer
 * Reduucer Class
 * @author harisyam_s
 * 
 */	
public class MMCSSummaryUploadsDownloadsReducer  extends Reducer<Text,LongWritable, Text, LongWritable> {
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws java.io.IOException ,InterruptedException {
		
		if(key!=null && !(key.toString().contains("null"))){
			// Iterating through the Values
			Iterator<LongWritable> it = values.iterator();
			long dbWritable = 0L;
			//For each values in the iterator calculate the sum
			while (it.hasNext()) {
				dbWritable= dbWritable+(it.next().get());
			}
			//Writing to the context
			context.write(key,new LongWritable(dbWritable));

		}
	}

}


