package git.snippets.mr.join;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PersonInfo implements WritableComparable<PersonInfo> {
    //1,张三,18,北京,flag
    public PersonInfo() {

    }
    private String id;
    private String name;
    private int age;
    private String address;
    private String flag;

    public PersonInfo(String id, String name, int age, String address, String flag) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.address = address;
        this.flag = flag;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "PersonInfo{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", address='" + address + '\'' +
                '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeUTF(this.name);
        out.writeInt(this.age);
        out.writeUTF(this.address);
        out.writeUTF(this.flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
         this.id = in.readUTF();
         this.name = in.readUTF();
         this.age = in.readInt();
         this.address = in.readUTF();
         this.flag = in.readUTF();
    }

    @Override
    public int compareTo(PersonInfo o) {
        return this.id.compareTo(o.id);
    }

}
