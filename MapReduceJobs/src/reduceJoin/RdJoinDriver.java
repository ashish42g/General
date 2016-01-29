package reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RdJoinDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 3){
            System.out.println("Usage: "+getClass().getSimpleName()+"<input dir1> <input dir2> <output dir>");
            return -1;
        }

		Job job = Job.getInstance(getConf(), "REDUCE SIDE JOIN");
		job.setJarByClass(RdJoinDriver.class);
		
		System.out.println(getConf().get("mapreduce.map.tasks"));

		Path txnPath = new Path(args[0]);
		Path custPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);

		MultipleInputs.addInputPath(job, txnPath, TextInputFormat.class, TransactionMapper.class);
		MultipleInputs.addInputPath(job, custPath, TextInputFormat.class, CustomerMapper.class);
		FileOutputFormat.setOutputPath(job,outputPath);

		return job.waitForCompletion(true) ? 0 :1 ;
}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new RdJoinDriver(), args));
	}

}
