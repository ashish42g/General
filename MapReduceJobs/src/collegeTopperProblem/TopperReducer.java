package collegeTopperProblem;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopperReducer extends Reducer<CllgStudentName, IntWritable, Text, Text> {

    @Override
    protected void reduce(CllgStudentName key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int totalMarks = 0;
        long avgMarks = 0;
        int marksCount = 0;
        long incFactor = 0;
        long lastMaxAvg = 0;

        for (IntWritable value : values) {
            totalMarks += value.get();
            marksCount++;
        }

        avgMarks = totalMarks / marksCount;
        long globaLastMaxAvg = context.getCounter("cllg_topper", "last_max_avg").getValue();

        /*if (lastMaxAvg == 0) {
            lastMaxAvg = avgMarks;
            context.getCounter("cllg_topper", "last_max_avg").increment(lastMaxAvg);
            context.getCounter("cllg_topper", "max_avg").increment(lastMaxAvg);
        } else if (avgMarks >  lastMaxAvg) {
            incFactor = avgMarks - lastMaxAvg;
            lastMaxAvg = avgMarks;
            context.getCounter("cllg_topper", "max_avg").increment(incFactor);
        }*/
        if (globaLastMaxAvg == 0) {
            context.getCounter("cllg_topper", "last_max_avg").increment(avgMarks);
            context.getCounter("cllg_topper", "max_avg").increment(avgMarks);
        } else if (avgMarks > globaLastMaxAvg) {
            incFactor = avgMarks - globaLastMaxAvg;
            context.getCounter("cllg_topper", "max_avg").increment(incFactor);
            context.getCounter("cllg_topper", "last_max_avg").increment(incFactor);
        }

        context.write(new Text(key.toString()), new Text(String.valueOf(avgMarks)));
    }
}
