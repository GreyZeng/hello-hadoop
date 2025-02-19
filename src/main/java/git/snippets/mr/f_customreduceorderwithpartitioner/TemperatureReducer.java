package git.snippets.mr.f_customreduceorderwithpartitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class TemperatureReducer extends Reducer<Temperature, Temperature, Temperature, NullWritable> {
    int cnt;
    String year;
    String month;
    String day;

    //用来标记某个分区中是否处理过相同日期数据，map中key为年月，value：<day,年月计数>
    HashMap<String, String> flagMap = new HashMap<>();

    //相同的key分为一组，这里需要将分区中所有的数据拿在一起最后比较获取日期最大的数据
    ArrayList<Temperature> list = new ArrayList<>();

    @Override
    protected void reduce(Temperature key, Iterable<Temperature> values, Reducer<Temperature, Temperature, Temperature, NullWritable>.Context context) throws IOException, InterruptedException {

        for (Temperature next : values) {
            list.add(next);
        }

        //最后比较得到温度较高的两条数据，日期不能相同
        for (Temperature temperature : list) {
            year = temperature.getYear();
            month = temperature.getMonth();
            day = temperature.getDay();

            //第一次处理某个年月日数据
            if (!flagMap.containsKey(year + "-" + month)) {
                cnt = 1;
                context.write(temperature, NullWritable.get());
                flagMap.put(year + "-" + month, day + "," + cnt);
            }

            //如果flagMap中包含年月数据，判断value是不是同一日期，是同一日期不输出，不是同一日期输出数据
            if (flagMap.containsKey(year + "-" + month) && !day.equals(flagMap.get(year + "-" + month).split(",")[0])) {
                //获取当前年月记录的条数
                cnt = Integer.parseInt(flagMap.get(year + "-" + month).split(",")[1]);
                cnt += 1;
                //说明当前年月下不够2条数据
                if (cnt == 2) {
                    context.write(temperature, NullWritable.get());
                }
                flagMap.put(year + "-" + month, day + "," + cnt);
            }

        }

    }

}
