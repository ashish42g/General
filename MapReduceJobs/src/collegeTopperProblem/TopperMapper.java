package collegeTopperProblem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Data Sample
 * <p>
 * college_name,branch_name,student_name,subject_name,marks
 * <p>
 * PCE,CS,Ashish garg,computer science,99
 * PCE,CS,Ashish garg,english,90
 **/

public class TopperMapper extends Mapper<LongWritable, Text, CllgStudentName, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split(",");

        if (words != null && words.length > 0) {
            context.write(new CllgStudentName(words[0], words[2]), new IntWritable(Integer.valueOf(words[4])));
        }
    }
}
