package reduceSideJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import reduceSideJoin.Record.RecType;

public class LocationMapper extends Mapper<LongWritable, Text, Text, Record>{
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Record record = parse(value);
		/*LocKey newKey = new LocKey();
		newKey.locId = record.locId;

		if (record.type == RecType.emp) {
			newKey.isLocation = false;
		} else {
			newKey.isLocation = true;
		}*/
		context.write(new Text(record.getLocId()), record);
	}

	private Record parse(Text value) {
		Record record = new Record();
		String line = value.toString();
		String words[] = line.split("\\t");
		if (words != null && words.length == 2) {
			record.setLocId(words[0]);
			record.setLocName(words[1]);
			record.setEmpId(null);
			record.setEmpName(null);
			record.setType(RecType.loc);
		}
		return record;
	}
}
