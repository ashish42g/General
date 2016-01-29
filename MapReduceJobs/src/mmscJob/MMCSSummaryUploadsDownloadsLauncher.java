package mmscJob;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Class Name: MMCSMRMigrationLauncher
 * Launcher Class
 * @author harisyam_s
 * 
 */	
public class MMCSSummaryUploadsDownloadsLauncher extends Configured implements Tool{
	
	private static String JOB_NAME="MMCS-DF-SUMMARY-UPLOADS-DOWNLOADS";
	private static int NO_OF_REDUCERS=10;
	private static int SUCCESS_CODE=0;
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new MMCSSummaryUploadsDownloadsLauncher(), args);
	}
	/**
	 * Class Name: run
	 * @author harisyam_s
	 * 
	 */	
	@Override
	public int run(String[] args) throws Exception {
		
		String INPUT_FOLDER=null;
		String DUMMY=null;
		String OUTPUT_FOLDER=null;
		//Input Arguments : INPUT FOLDER , na , OUTPUTFOLDER
		if(args.length<3) {
			throw new Exception("Invalid arguments");
		}
		INPUT_FOLDER = args[0];
		DUMMY = args[1];
		OUTPUT_FOLDER = args[2];
		
		if((INPUT_FOLDER.length()<=0)||(DUMMY.length()<=0)||(OUTPUT_FOLDER.length()<=0)) {
			throw new Exception("Invalid arguments");
		}
		//Setting the Configuration for MR Job
		Configuration firstJobconf = getConf();
		firstJobconf.set("mapred.job.priority", "NORMAL");
		firstJobconf.set("mapred.compress.map.output", "false");
		firstJobconf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");
		firstJobconf.set("mapred.output.compress", "false");
		firstJobconf.set("mapred.output.compression","org.apache.hadoop.io.compress.SnappyCodec");
		
		//Configuring the JOB Name
		Job firstJob = new Job(firstJobconf,JOB_NAME);

		FileInputFormat.setInputPaths(firstJob, new Path(INPUT_FOLDER));
		FileOutputFormat.setOutputPath(firstJob, new Path(OUTPUT_FOLDER));
		firstJob.setJarByClass(MMCSSummaryUploadsDownloadsLauncher.class);
		// Setting Map Output Key Class
		firstJob.setMapOutputKeyClass(Text.class);
		// Setting Map Output Value Class
		firstJob.setMapOutputValueClass(LongWritable.class);
		firstJob.setOutputKeyClass(Text.class);
		firstJob.setOutputValueClass(LongWritable.class);
		
		// Setting No of Reducer Count
		firstJob.setNumReduceTasks(NO_OF_REDUCERS);
		firstJob.setInputFormatClass(TextInputFormat.class);
		firstJob.setOutputFormatClass(TextOutputFormat.class);
		// Setting Mapper , Combiner and Reducer Classes
		firstJob.setMapperClass(MMCSSummaryUploadsDownloadsMapper.class);
		firstJob.setCombinerClass(MMCSSummaryUploadsDownloadsReducer.class);
		firstJob.setReducerClass(MMCSSummaryUploadsDownloadsReducer.class);
		
		boolean success = firstJob.waitForCompletion(true);
		
		if(success)
		{
		return SUCCESS_CODE;
		}
		else
		{
			return 1;
		}
		
	}
}
