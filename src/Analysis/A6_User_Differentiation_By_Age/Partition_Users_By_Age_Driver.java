/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A6_User_Differentiation_By_Age;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class Partition_Users_By_Age_Driver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Users by Age");
        job.setJarByClass(Partition_Users_By_Age_Driver.class);

        job.setMapperClass(Partition_Users_By_Age_Mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // partitioner class inclusion
        job.setPartitionerClass(Partition_Users_By_Age_Partitioner.class);

        // set multiple formats for custom naming partitioning
        MultipleOutputs.addNamedOutput(job, "ageBins", TextOutputFormat.class,  Text.class, NullWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);


        //11-17, 18-25, 26-35, 36-49,50-65,66-80, 81-99

        // set num of reduce tasks based on partition we need (here we need 10 cos total no.of countries)
        job.setNumReduceTasks(8);
        job.setReducerClass(Partition_Users_By_Age_Reducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

