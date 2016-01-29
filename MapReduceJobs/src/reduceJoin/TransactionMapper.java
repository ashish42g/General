package reduceJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author ashish
 * TxnId	CustomerId	Amount	TxnType
 * 2		2			500		BigBazar
 * 3		2			700		BurgerKing
 */
public class TransactionMapper extends Mapper<LongWritable, Text, TextPair, Text>{
	@SuppressWarnings("unused")
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = line.split("\\t");
	}

}
