package secondarySort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondarySortReducer extends Reducer<StringPair, Text, Text, Text> {
	@Override
	protected void reduce(StringPair key, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		context.write(new Text(""), value.iterator().next());
	}
}
