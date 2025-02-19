package git.snippets.mr.f_customreducewithgroup;

import git.snippets.mr.f_customreduceorderwithpartitioner.Temperature;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TempertureReducer extends Reducer<Temperature, Temperature, Temperature, NullWritable> {
    @Override
    protected void reduce(Temperature key, Iterable<Temperature> values, Reducer<Temperature, Temperature, Temperature, NullWritable>.Context context) throws IOException, InterruptedException {
        String day = "";//记录日期天
        int cnt = 0;//记录条数
        for (Temperature next : values) {
            if (cnt == 0) {
                context.write(key, NullWritable.get());
                day = next.getDay();
                cnt += 1;
            }
            if (cnt != 0 && !day.equals(next.getDay())) {
                context.write(key, NullWritable.get());
                break;
            }

        }


    }
}