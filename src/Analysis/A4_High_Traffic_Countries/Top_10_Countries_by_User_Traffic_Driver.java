/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A4_High_Traffic_Countries;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Top_10_Countries_by_User_Traffic_Driver {

    /**
     * @param args the command line arguments
     */

    public static void main(String[] args) throws 
            IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 10 Countries by User Traffic");
        job.setJarByClass(Top_10_Countries_by_User_Traffic_Driver.class);
        job.setMapperClass(Top_10_Countries_by_User_Traffic_Mapper.class);

        job.setCombinerClass(Top_10_Countries_by_User_Traffic_Reducer.class);
        job.setReducerClass(Top_10_Countries_by_User_Traffic_Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
