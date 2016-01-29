package reduceSideJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceJoinDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new ReduceJoinDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n",getClass().getSimpleName());
			return -1;
		}

		Job job = Job.getInstance(getConf(), "REDUCE SIDE JOIN");
		job.setJarByClass(ReduceJoinDriver.class);

		Path employeeInputPath = new Path(args[0]);
		Path locationsInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		MultipleInputs.addInputPath(job, employeeInputPath, TextInputFormat.class, EmployeeMapper.class);
		MultipleInputs.addInputPath(job, locationsInputPath, TextInputFormat.class, EmployeeMapper.class );
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(EmployeeMapper.class);
		/*job.setMapperClass(LocationMapper.class);*/

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Record.class);

		/*job.setSortComparatorClass(LocationKeySortComparator.class);
		job.setGroupingComparatorClass(LocKeyGroupingComparator.class);*/

		job.setReducerClass(ReduceSideJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
