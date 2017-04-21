/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Filtered_.A4_User_Traffic_By_Country;

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
public class Total_Traffic_by_Country_Driver {

    /**
     * @param args the command line arguments
     */

    public static void main(String[] args) throws 
            IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Total Traffic by Country");
        job.setJarByClass(Total_Traffic_by_Country_Driver.class);
        job.setMapperClass(Total_Traffic_by_Country_Mapper.class);

        job.setCombinerClass(Total_Traffic_by_Country_Reducer.class);
        job.setReducerClass(Total_Traffic_by_Country_Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
