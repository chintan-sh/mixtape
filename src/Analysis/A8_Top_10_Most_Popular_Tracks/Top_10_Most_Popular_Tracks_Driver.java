/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A8_Top_10_Most_Popular_Tracks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Top_10_Most_Popular_Tracks_Driver {

    /**
     * @param args the command line arguments
     */

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 10 most popular tracks ");
        job.setJarByClass(Top_10_Most_Popular_Tracks_Driver.class);

        job.setMapperClass(Top_10_Most_Popular_Tracks_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);
        job.setReducerClass(Top_10_Most_Popular_Tracks_Reducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
