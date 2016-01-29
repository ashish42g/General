package appleObjectCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustPart extends Partitioner<StringPair, Text>  {

    @Override
    public int getPartition(StringPair pair, Text value, int numReduceTask) {
        return (pair.getCount().hashCode() % numReduceTask);
    }
}
