package git.snippets.mr.d_customsort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区内实现排序
 * 这个例子中MapReduce 排序案例和二次排序案例中都是全排序，最终由一个ReduceTask进行处理并生成1个文件，如果数据量大这样的任务运行将会非常慢，一般可以按照一些字段进行分区，由多个ReduceTask进行处理即可。
 * 将以上案例中的数据结果按照日期进行分组，最终每个日期数据形成1个文件，并在文件内形成二次排序，先按照消费价格排序，然后再按照购买数量排序。这就需要自定义分区器并在Driver代码内设置使用自定义分区器及ReduceTask的个数。
 */
public class OrderPartitioner extends Partitioner<Order, Text> {
    // 分区内排序
    @Override
    public int getPartition(Order order, Text text, int numPartitions) {
        //先获取dt
        String dt = order.getDt();
        return (dt.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
