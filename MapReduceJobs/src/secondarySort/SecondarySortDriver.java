package secondarySort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),new SecondarySortDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", getClass().getSimpleName());
			return -1;
		}

		Job job = Job.getInstance(getConf(), "SECONDARY SORT");
		job.setJarByClass(SecondarySortDriver.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));

		job.setMapperClass(SecondarySortMapper.class);
		job.setReducerClass(SecondarySortReducer.class);

		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(SecondarySortPartioner.class);
		job.setSortComparatorClass(SecondarySortComparator.class);
		job.setGroupingComparatorClass(SecondarGroupComparator.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

}
