package git.snippets.mr.temperature;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义Reduce端分组比较器
 * 1) 自定义类要继承 WritableComparator
 * 2) 自定义类中实现 构造，在构造中调用父构造来创建实例
 * 3) 自定义类中实现 compare 方法
 */
public class TemperatureGroupingComparator extends WritableComparator {
    public TemperatureGroupingComparator() {
        super(Temperature.class, true);
    }

    // 把同样年月的数据放到一个reduce任务里面
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //相同年月数据放在一起
        Temperature a1 = (Temperature) a;
        Temperature b1 = (Temperature) b;

        int yearCompare = a1.getYear().compareTo(b1.getYear());
        if (yearCompare == 0) {
            return a1.getMonth().compareTo(b1.getMonth());
        }
        return yearCompare;

    }
}
