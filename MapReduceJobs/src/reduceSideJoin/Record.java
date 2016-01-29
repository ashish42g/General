package reduceSideJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Record implements Writable {

	enum RecType {
		emp, loc
	};
	RecType type;

	String locId;
	String empId;
	String empName;
	String locName;

	@Override
	public void write(DataOutput out) throws IOException {

		if (locId != null) {
			out.writeUTF(locId);
		} else {
			out.writeUTF("");
		}
		if (empId != null) {
			out.writeUTF(empId);
		} else {
			out.writeUTF("");
		}
		if (empName != null) {
			out.writeUTF(empName);
		} else {
			out.writeUTF("");
		}
		if (locName != null) {
			out.writeUTF(locName);
		} else {
			out.writeUTF("");
		}
		out.writeUTF(type.toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		locId = in.readUTF();
		empId = in.readUTF();
		empName = in.readUTF();
		locName = in.readUTF();
		type = RecType.valueOf(in.readUTF());
	}

	@Override
	public String toString() {
		return "Record [type=" + type + ", locId=" + locId + ", empId=" + empId
				+ ", empName=" + empName + ", locName=" + locName + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((empId == null) ? 0 : empId.hashCode());
		result = prime * result + ((empName == null) ? 0 : empName.hashCode());
		result = prime * result + ((locId == null) ? 0 : locId.hashCode());
		result = prime * result + ((locName == null) ? 0 : locName.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		Record other = (Record) obj;
		if (empId == null) {
			if (other.empId != null)
				return false;
		} else if (!empId.equals(other.empId))
			return false;
		if (empName == null) {
			if (other.empName != null)
				return false;
		} else if (!empName.equals(other.empName))
			return false;
		if (locId == null) {
			if (other.locId != null)
				return false;
		} else if (!locId.equals(other.locId))
			return false;
		if (locName == null) {
			if (other.locName != null)
				return false;
		} else if (!locName.equals(other.locName))
			return false;
		if (type != other.type)
			return false;
		return true;
	}

	public RecType getType() {
		return type;
	}

	public void setType(RecType type) {
		this.type = type;
	}

	public String getEmpId() {
		return empId;
	}

	public void setEmpId(String empId) {
		this.empId = empId;
	}

	public String getEmpName() {
		return empName;
	}

	public void setEmpName(String empName) {
		this.empName = empName;
	}

	public String getLocId() {
		return locId;
	}

	public void setLocId(String locId) {
		this.locId = locId;
	}

	public String getLocName() {
		return locName;
	}

	public void setLocName(String locName) {
		this.locName = locName;
	}

}