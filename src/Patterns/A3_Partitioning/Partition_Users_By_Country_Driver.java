/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns.A3_Partitioning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Partition_Users_By_Country_Driver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Users by Country");
        job.setJarByClass(Partition_Users_By_Country_Driver.class);

        job.setMapperClass(Partition_Users_By_Country_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // partitioner class inclusion
        job.setPartitionerClass(Partition_Users_By_Country_Partitioner.class);

        // set multiple formats for custom naming partitioning
        MultipleOutputs.addNamedOutput(job, "countryBins", TextOutputFormat.class,  Text.class, NullWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);

        // set num of reduce tasks based on partition we need (here we need 10 cos total no.of countries)
        job.setNumReduceTasks(11);
        job.setReducerClass(Partition_Users_By_Country_Reducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



//job.setOutputKeyClass(Text.class);
//job.setOutputValueClass(NullWritable.class);