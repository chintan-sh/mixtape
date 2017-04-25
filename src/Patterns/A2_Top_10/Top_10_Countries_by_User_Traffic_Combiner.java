/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns.A2_Top_10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Top_10_Countries_by_User_Traffic_Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    public IntWritable sum = new IntWritable();


    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for( IntWritable value : values) {
                count += value.get();
            }

            sum.set(count);
            context.write(key, sum);


    }



}

