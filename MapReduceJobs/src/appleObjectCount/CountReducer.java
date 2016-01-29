package appleObjectCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class CountReducer extends Reducer<StringPair, Text, Text, Text> {

    int count = 0;

    @Override
    protected void reduce(StringPair key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //for (Text value : values) {
            //if (count < 3) {
            System.out.println("key "+key.getName()+ " value  "+key.getCount());
                context.write(new Text(key.toString()), new Text(key.getCount()));
             //   count ++;
            //}
        //}
    }

    /*@Override
    public void run(Context context) throws IOException, InterruptedException {
        super.run(context);
    }*/

    public void run(Context context) throws IOException, InterruptedException {
        super.setup(context);
        while (context.nextKey() && count++ < 5) {
            reduce(context.getCurrentKey(), context.getValues(), context);
        }
        super.cleanup(context);
    }
}
