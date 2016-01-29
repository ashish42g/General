package secondarySort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartioner extends Partitioner<StringPair, Text> {

	@Override
	public int getPartition(StringPair pair, Text value, int numReduceTask) {
		return (pair.getFirstName().hashCode() % numReduceTask);
	}

}
