package git.snippets.mr.customorder;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义类 ，实现 WritableComparable 接口
 */
public class Order implements WritableComparable<Order> {
    public Order() {

    }

    //1001	2024-03-10	商品A	2	100
    private String orderId;
    private String dt;
    private String productName;
    private int amount;
    private double totalCost;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getDt() {
        return dt;
    }

    public void setDt(String dt) {
        this.dt = dt;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public double getTotalCost() {
        return totalCost;
    }

    public void setTotalCost(double totalCost) {
        this.totalCost = totalCost;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", dt='" + dt + '\'' +
                ", productName='" + productName + '\'' +
                ", amount=" + amount +
                ", totalCost=" + totalCost +
                '}';
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(dt);
        out.writeUTF(productName);
        out.writeInt(amount);
        out.writeDouble(totalCost);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.dt = in.readUTF();
        this.productName = in.readUTF();
        this.amount = in.readInt();
        this.totalCost = in.readDouble();
    }

    //实现按照花费金额降序排序
    @Override
    public int compareTo(Order o) {
        if(this.totalCost > o.totalCost){
            return -1;
        }else if(this.totalCost < o.totalCost){
            return 1;
        }else{
            //如果两个order 价格相等，按照数量降序排序
            if(this.amount > o.amount){
                return -1;
            }else if(this.amount < o.amount){
                return 1;
            }else {
                return 0;
            }

        }
    }
}
