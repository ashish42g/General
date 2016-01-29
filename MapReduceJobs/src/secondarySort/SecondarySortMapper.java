package secondarySort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends
		Mapper<LongWritable, Text, StringPair, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] words = line.split("\\t");
		if (words != null && words.length > 0) {
			if (words[0] != null && words[0].length() > 0) {
				if (words[2] != null && words[2].length() > 0) {
					context.write(
							new StringPair(words[0], words[2].substring(0, 3)),
							new Text(line));
				}
			}
		}
	}

}
