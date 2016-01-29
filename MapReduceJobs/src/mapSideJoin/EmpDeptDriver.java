package mapSideJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EmpDeptDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if(args.length != 2){
            System.out.println("Usage: "+getClass().getSimpleName()+"<input dir1> <input dir2> <output dir>");
            return -1;
        }

        Job job = Job.getInstance(getConf(), "Map Side Join");
        job.setJarByClass(EmpDeptDriver.class);

        Path empPath = new Path(args[0]);
        String deptPath = "/Users/ashish/Documents/sample_data/map_side_join/DeptDataMap.txt";

        FileInputFormat.setInputPaths(job, empPath);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new Path(deptPath).toUri());

        job.setMapperClass(EmployeeMapper.class);

        /*job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);*/

        job.setNumReduceTasks(0);

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new Configuration(), new EmpDeptDriver(), args));
    }
}
