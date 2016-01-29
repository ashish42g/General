package collegeTopperProblem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopperDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new Configuration(), new TopperDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", getClass().getSimpleName());
            return -1;
        }

        Job job = Job.getInstance(getConf(), "COLLEGE TOPPER PROBLEM");
        job.setJarByClass(TopperDriver.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,  new Path(args[1]));

        job.setMapperClass(TopperMapper.class);
        job.setMapOutputKeyClass(CllgStudentName.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TopperReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(3);

        boolean success = job.waitForCompletion(true);
        System.out.println("topper_avg "+job.getCounters().findCounter("cllg_topper", "max_avg").getValue());
        CounterGroup maxAvggroup = job.getCounters().getGroup("cllg_topper");
        long maxVal = 0;
        for (Counter avg : maxAvggroup){
             if (avg.getValue() > maxVal){
              maxVal = avg.getValue();
             }
        }
        System.out.println("max value :::::::::::::::" + maxVal);
        return success ? 0 : 1;
    }

}
