package reduceSideJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class LocKey implements WritableComparable<LocKey> {

	String locId;
	boolean isLocation;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(locId);
		out.writeBoolean(isLocation);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		locId = in.readUTF();
		isLocation = in.readBoolean();
	}

	@Override
	public int compareTo(LocKey k) {
		if (locId != k.locId) {
			return locId.compareTo(k.locId);
		} else {
			return Boolean.compare(isLocation, k.isLocation);
		}
	}

	@Override
	public int hashCode() {
		return Integer.valueOf(locId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LocKey other = (LocKey) obj;
		if (isLocation != other.isLocation)
			return false;
		if (locId != other.locId)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "LocationKey [locId=" + locId + ", isLocation=" + isLocation
				+ "]";
	}

	public String getLocId() {
		return locId;
	}

	public void setLocId(String locId) {
		this.locId = locId;
	}

	public boolean isLocation() {
		return isLocation;
	}

	public void setLocation(boolean isLocation) {
		this.isLocation = isLocation;
	}

}
