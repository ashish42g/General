package reduceSideJoin;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparator;

public class LocKeyGroupingComparator extends WritableComparator {

	@Override
	public int compare(Object a, Object b) {
		LocKey k1 = (LocKey) a;
		LocKey k2 = (LocKey) b;
		return k1.getLocId().compareTo(k2.getLocId());
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		DataInput stream1 = new DataInputStream(new ByteArrayInputStream(b1,
				s1, l1));
		DataInput stream2 = new DataInputStream(new ByteArrayInputStream(b2,
				s2, l2));

		LocKey v1 = new LocKey();
		LocKey v2 = new LocKey();

		try {
			v1.readFields(stream1);
			v2.readFields(stream2);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return compare(v1, v2);
	}
}
