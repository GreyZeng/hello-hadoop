package git.snippets.mr.f_customreduceorderwithpartitioner;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器，将相同年月数据放在同一个分区内
 */
public class TemperaturePartitioner extends Partitioner<Temperature, Temperature> {
    @Override
    public int getPartition(Temperature key, Temperature value, int numPartitions) {
        String ym = key.getYear() + "-" + key.getMonth();
        return (ym.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
