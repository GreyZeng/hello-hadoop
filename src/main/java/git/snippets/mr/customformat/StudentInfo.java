package git.snippets.mr.customformat;



import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StudentInfo implements WritableComparable<StudentInfo> {
    public StudentInfo() {
    }
    private String name;
    private int score;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @Override
    public String toString() {
        return "StudentInfo{" +
                "name='" + name + '\'' +
                ", score=" + score +
                '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(score);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.score = in.readInt();
    }

    @Override
    public int compareTo(StudentInfo o) {
        if(this.score > o.score){
            return -1;
        }else if(this.score <o.score){
            return 1;
        }else{
            return 0;
        }

    }
}
