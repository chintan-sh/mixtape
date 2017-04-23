/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Summarized_.A5_User_Partition_By_Country;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Top_20_Traffic_Mapper_1 extends Mapper<Object, Text, Text, IntWritable> {
    private Text country;
    private IntWritable count = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get user info
        String[] userInfo = value.toString().split(" ");

        // extract artist name
        country = new Text(userInfo[0]);
        count = new IntWritable(Integer.parseInt(userInfo[1]));

        context.write(country, count);
    }
}
