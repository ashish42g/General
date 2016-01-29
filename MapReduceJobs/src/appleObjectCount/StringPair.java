package appleObjectCount;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringPair implements WritableComparable<StringPair> {

    String name = null;
    //String value = null;
    String count = null;

    public StringPair() {
    }

    public StringPair(String name/*, String value*/, String count) {
        this.name = name;
        //this.value = value;
        this.count = count;
    }

    @Override
    public int compareTo(StringPair object) {

        int cnt1 = Integer.valueOf(count);
        int cnt2 = Integer.valueOf(object.count);

        if (cnt1 > cnt2)
            return -1;
        else /*if (cnt1 < cnt2)*/
            return 1;
        /*else
            return 0;*/
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        //dataOutput.writeUTF(value);
        dataOutput.writeUTF(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        //value = dataInput.readUTF();
        count = dataInput.readUTF();
    }

    public String getName() {
        return name;
    }

    public String getCount() {
        return count;
    }

    public void setName(String t) {
        this.name = t;
    }

    public void setCount(String count) {
        this.count = count;
    }

/*
    public String getValue() {
        return value;
    }
*/

    /*public void setValue(String value) {
        this.value = value;
    }*/

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StringPair that = (StringPair) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        //if (value != null ? !value.equals(that.value) : that.value != null) return false;
        return !(count != null ? !count.equals(that.count) : that.count != null);

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        //result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (count != null ? count.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return name;
    }
}
