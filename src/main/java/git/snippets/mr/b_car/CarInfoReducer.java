package git.snippets.mr.b_car;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class CarInfoReducer extends Reducer<Text, CarInfo, NullWritable, CarInfo> {
    private final CarInfo carInfo = new CarInfo();
    private static final DecimalFormat df = new DecimalFormat("#.00");

    @Override
    protected void reduce(Text key, Iterable<CarInfo> values, Reducer<Text, CarInfo, NullWritable, CarInfo>.Context context) throws IOException, InterruptedException {
        //定义变量记录当前car的条数
        int cnt = 0;
        //总速度
        double totalSpeed = 0;
        //总里程数
        double totalKm = 0;
        for (CarInfo value : values) {
            totalSpeed += value.getAvgSpeed();
            totalKm += value.getTotalKm();
            cnt++;
        }
        //平均速度
        Double avgSpeed = Double.valueOf(df.format(totalSpeed / cnt));
        //组织写出对象
        carInfo.setCarNum(key.toString());
        carInfo.setAvgSpeed(avgSpeed);
        carInfo.setTotalKm(totalKm);
        context.write(NullWritable.get(), carInfo);
    }
}
