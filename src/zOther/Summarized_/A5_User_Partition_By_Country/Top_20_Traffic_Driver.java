/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Summarized_.A5_User_Partition_By_Country;

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
public class Top_20_Traffic_Driver {

    /**
     * @param args the command line arguments
     */

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Total traffic by country");
        job1.setJarByClass(Top_20_Traffic_Driver.class);

        // the usual - get basic mapred ready
        job1.setMapperClass(Top_20_Traffic_Mapper.class);
        job1.setCombinerClass(Top_20_Traffic_Reducer.class);
        job1.setReducerClass(Top_20_Traffic_Reducer.class);

        // this will basically out -> country, count
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        boolean complete = job1.waitForCompletion(true);

        // here's where we sort
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Top 20 Traffic by Country");
        if (complete) {
            job2.setJarByClass(Top_20_Traffic_Driver.class);

            // namesake fellow, take it and go types - mostly useless
            job2.setMapperClass(Top_20_Traffic_Mapper_1.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);

            // this is where we would ideally sort descendingly
            job2.setSortComparatorClass(Top_20_Traffic_SortComparator.class);

            // o/p top 20, man
            job2.setNumReduceTasks(1);
            job2.setReducerClass(Top_20_Traffic_Reducer_1.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}

