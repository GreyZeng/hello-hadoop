package git.snippets.mr.d_customsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, Order, Text> {
    //创建Order对象
    Order order = new Order();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Order, Text>.Context context) throws IOException, InterruptedException {
        //1001	2024-03-10	商品A	2	100
        String line = value.toString();
        String[] split = line.split("\t");
        order.setOrderId(split[0]);
        order.setDt(split[1]);
        order.setProductName(split[2]);
        order.setAmount(Integer.parseInt(split[3]));
        order.setTotalCost(Double.parseDouble(split[4]));

        context.write(order, value);

    }
}
