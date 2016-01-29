package secondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class StringPair implements WritableComparable<StringPair>{

	String firstName = null;
	String birthYear = null;

	public StringPair() {
	}

	public StringPair(String firstName, String birthYear) {
		super();
		this.firstName = firstName;
		this.birthYear = birthYear;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		firstName = in.readUTF();
		birthYear = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(firstName);
		out.writeUTF(birthYear);
	}

	@Override
	public int compareTo(StringPair o) {
		int ret = firstName.compareTo(o.firstName);
		if (ret == 0) {
			return birthYear.compareTo(o.birthYear);
		}
		return ret;
	}

	@Override
	public String toString() {
		return "StringPair [firstName=" + firstName + ", birthYear="
				+ birthYear + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((birthYear == null) ? 0 : birthYear.hashCode());
		result = prime * result
				+ ((firstName == null) ? 0 : firstName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StringPair other = (StringPair) obj;
		if (birthYear == null) {
			if (other.birthYear != null)
				return false;
		} else if (!birthYear.equals(other.birthYear))
			return false;
		if (firstName == null) {
			if (other.firstName != null)
				return false;
		} else if (!firstName.equals(other.firstName))
			return false;
		return true;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getBirthYear() {
		return birthYear;
	}

	public void setBirthYear(String birthYear) {
		this.birthYear = birthYear;
	}

}
