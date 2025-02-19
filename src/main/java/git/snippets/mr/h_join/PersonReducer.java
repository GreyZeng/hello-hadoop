package git.snippets.mr.h_join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class PersonReducer extends Reducer<PersonInfo, PersonInfo, PersonInfo, NullWritable> {
    @Override
    protected void reduce(PersonInfo key, Iterable<PersonInfo> values, Reducer<PersonInfo, PersonInfo, PersonInfo, NullWritable>.Context context) throws IOException, InterruptedException {
        /**
         * 1,张三,18
         * 1,北京
         * 2,李四,19
         * 2,上海
         * 6,高八,23
         * 7,杭州
         */
        ArrayList<PersonInfo> personList = new ArrayList<>();
        String address = "";
        for (PersonInfo value : values) {
            System.out.println("key = " + key);
            if (value.getFlag().equals("address")) {
                address = value.getAddress();
            }
            personList.add(new PersonInfo(value.getId(), value.getName(), value.getAge(), value.getAddress(), null));
            System.out.println("personList size " + personList.size());
        }

        for (PersonInfo personInfo : personList) {
            System.out.println("设置地址for循环 = " + key);
            personInfo.setAddress(address);
            if (!personInfo.getName().isEmpty() && !personInfo.getAddress().isEmpty()) {
                context.write(personInfo, NullWritable.get());
            }
        }

    }
}
