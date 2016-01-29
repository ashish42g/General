package secondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarGroupComparator extends WritableComparator {

	public SecondarGroupComparator() {
		super(StringPair.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable v1, WritableComparable v2) {
		StringPair pair1 = (StringPair) v1;
		StringPair pair2 = (StringPair) v2;

		return pair1.getFirstName().compareTo(pair2.getFirstName());
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
