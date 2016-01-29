package collegeTopperProblem;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CllgStudentName implements WritableComparable<CllgStudentName> {

    String collegeName = null;
    String studentName = null;

    public CllgStudentName() {
    }

    public CllgStudentName(String collegeName, String studentName) {
        this.collegeName = collegeName;
        this.studentName = studentName;
    }

    public void setCollegeName(String collegeName) {
        this.collegeName = collegeName;
    }

    public void setStudentName(String studentName) {
        this.studentName = studentName;
    }

    public String getStudentName() {
        return studentName;
    }

    public String getCollegeName() {

        return collegeName;
    }

    @Override
    public int compareTo(CllgStudentName object) {
        int ret = collegeName.compareTo(object.collegeName);
        if (ret == 0){
            return studentName.compareTo(object.studentName);
        }
        return ret;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(collegeName);
        dataOutput.writeUTF(studentName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        collegeName = dataInput.readUTF();
        studentName = dataInput.readUTF();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CllgStudentName that = (CllgStudentName) o;

        if (collegeName != null ? !collegeName.equals(that.collegeName) : that.collegeName != null) return false;
        return !(studentName != null ? !studentName.equals(that.studentName) : that.studentName != null);

    }

    @Override
    public int hashCode() {
        int result = collegeName != null ? collegeName.hashCode() : 0;
        result = 31 * result + (studentName != null ? studentName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "collegeName : " + collegeName + "\t" + "studentName : " + studentName;
    }
}
