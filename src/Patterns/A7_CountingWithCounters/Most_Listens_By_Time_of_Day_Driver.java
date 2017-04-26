/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns.A7_CountingWithCounters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Most_Listens_By_Time_of_Day_Driver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most listens by Time of the Day");
        job.setJarByClass(Most_Listens_By_Time_of_Day_Driver.class);

        job.setMapperClass(Most_Listens_By_Time_of_Day_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int code = job.waitForCompletion(true) ? 0 : 1;

        if( code == 0 ){
            for(Counter counter : job.getCounters().getGroup(Most_Listens_By_Time_of_Day_Mapper.HOUR_COUNTER_GROUP)){
                System.out.println(counter.getDisplayName() + "\t" + counter.getValue());
            }
        }

        FileSystem.get(conf).delete(new Path(args[1]), true);

        System.exit(code);
    }
}















/*
0	750333
1	683450
10	666702
11	730988
12	781963
13	851940
14	922142
15	992910
16	1051174
17	1083863
18	1120495
19	1132132
2	644376
20	1114398
21	1049397
22	949895
23	840368
3	610452
4	564569
5	517194
6	484337
7	489678
8	526726
9	591386
 */