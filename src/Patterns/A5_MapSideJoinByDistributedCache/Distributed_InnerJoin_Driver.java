/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns.A5_MapSideJoinByDistributedCache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * @author Chintan
 */
public class Distributed_InnerJoin_Driver {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inner Join User/Artist Data");
        job.setJarByClass(Distributed_InnerJoin_Driver.class);

        job.setMapperClass(Distributed_InnerJoin_Mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        try {
            job.addCacheFile(new URI("/home/chintan/IdeaProjects/AdvancedDBMS/music-project/inputUserTaste/userid-profile.tsv#user"));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job , new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }
}
