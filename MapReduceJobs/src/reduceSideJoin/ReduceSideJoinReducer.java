package reduceSideJoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import reduceSideJoin.Record.RecType;

public class ReduceSideJoinReducer extends Reducer<Text, Record, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Record> values, Context context) throws IOException, InterruptedException {

		String thisLocation = null;
		List<Record> employee = new ArrayList<Record>();

		for (Record record : values) {
			if (record.type == RecType.loc) {
				thisLocation = record.getLocName();
			} else {
				employee.add(record);
			}
		}

		for (Record e : employee) {
			if (e.type == RecType.emp) {
				e.setLocName(thisLocation);
				context.write(new Text(""), new Text(e.toString()));
			}
		}

	}
}
