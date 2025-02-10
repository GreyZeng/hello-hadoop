package git.snippets.mr.customorder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<Order, Text> {
    // 分区内排序
    @Override
    public int getPartition(Order order, Text text, int numPartitions) {
        //先获取dt
        String dt = order.getDt();
        return (dt.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
