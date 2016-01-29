package appleObjectCount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComp extends WritableComparator {

    public GroupComp() {
        super(StringPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        StringPair pair1 = (StringPair) a;
        StringPair pair2 = (StringPair) b;

        return pair1.compareTo(pair2);

    }
}
