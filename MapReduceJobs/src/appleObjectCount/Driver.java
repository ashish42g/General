package appleObjectCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", getClass().getSimpleName());
            return -1;
        }

        Job job = Job.getInstance(getConf(), "UNIQUE OBJECT COUNT JOB");
        job.setJarByClass(Driver.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,  new Path(args[1]));

        job.setMapperClass(ObjectMapper.class);
        job.setMapOutputKeyClass(StringPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

}