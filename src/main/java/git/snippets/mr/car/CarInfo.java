package git.snippets.mr.car;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CarInfo implements Writable {
    //空构造
    public CarInfo() {
    }

    //属性
    private String car;
    private Double avgSpeed;
    private Double totalKm;

    public String getCar() {
        return car;
    }

    public void setCar(String car) {
        this.car = car;
    }

    public Double getAvgSpeed() {
        return avgSpeed;
    }

    public void setAvgSpeed(Double avgSpeed) {
        this.avgSpeed = avgSpeed;
    }

    public Double getTotalKm() {
        return totalKm;
    }

    public void setTotalKm(Double totalKm) {
        this.totalKm = totalKm;
    }

    //toString

    @Override
    public String toString() {
        return "car='" + car + '\'' +
                ", avgSpeed=" + avgSpeed +
                ", totalKm=" + totalKm ;
    }

    //序列化数据
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(car);
        out.writeDouble(avgSpeed);
        out.writeDouble(totalKm);
    }

    //反序列化数据
    @Override
    public void readFields(DataInput in) throws IOException {
        this.car = in.readUTF();
        this.avgSpeed = in.readDouble();
        this.totalKm = in.readDouble();
    }
}
