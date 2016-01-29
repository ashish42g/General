package secondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortComparator extends WritableComparator {
	
	public SecondarySortComparator() {
		super(StringPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		StringPair pair1 = (StringPair) a;
		StringPair pair2 = (StringPair) b;

		int ret = pair1.getFirstName().compareTo(pair2.getFirstName());
		if (ret == 0) {
			return -1 * (pair1.getBirthYear().compareTo(pair2.getBirthYear()));
		}
		return ret;
	}

	/*@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		DataInput stream1 = new DataInputStream(new ByteArrayInputStream(b1,
				s1, l1));
		DataInput stream2 = new DataInputStream(new ByteArrayInputStream(b2,
				s2, l2));

		StringPair v1 = new StringPair();
		StringPair v2 = new StringPair();

		try {
			v1.readFields(stream1);
			v2.readFields(stream2);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return compare(v1, v2);
	}*/

}
