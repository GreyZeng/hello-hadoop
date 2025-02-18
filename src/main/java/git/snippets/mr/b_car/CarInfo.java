package git.snippets.mr.b_car;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CarInfo implements Writable {
//    //空构造
//	// 这个是必须的，否则会报错
//    public CarInfo() {
//    }

    //属性
    private String carNum;
    private Double avgSpeed;
    private Double totalKm;

    public String getCarNum() {
        return carNum;
    }

    public void setCarNum(String carNum) {
        this.carNum = carNum;
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
        return "carNum='" + carNum + '\'' +
                ", avgSpeed=" + avgSpeed +
                ", totalKm=" + totalKm ;
    }

    //序列化数据
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(carNum);
        out.writeDouble(avgSpeed);
        out.writeDouble(totalKm);
    }

    //反序列化数据
    @Override
    public void readFields(DataInput in) throws IOException {
        this.carNum = in.readUTF();
        this.avgSpeed = in.readDouble();
        this.totalKm = in.readDouble();
    }
}
