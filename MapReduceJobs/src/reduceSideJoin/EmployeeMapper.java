package reduceSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import reduceSideJoin.Record.RecType;

/**
 * @author ashish Sample Date Employee data : 001 Elizabeth Windsor 4 
 * 							  Location data : 4 London
 */
public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Record> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Record record = parse(value);
		if (record != null) {
			context.write(new Text(record.getLocId()), record);
		}
	}

	private Record parse(Text value) {
		Record record = new Record();
		String line = value.toString();
		String words[] = line.split("\\t");
		if (words != null && words.length == 3) {
			record.setEmpId(words[0]);
			record.setEmpName(words[1]);
			record.setLocId(words[2]);
			record.setLocName(null);
			record.setType(RecType.emp);
		} else if (words != null && words.length == 2) {
			record.setEmpId(null);
			record.setEmpName(null);
			record.setLocId(words[0]);
			record.setLocName(words[1]);
			record.setType(RecType.loc);
		}
		return record;
	}
}
