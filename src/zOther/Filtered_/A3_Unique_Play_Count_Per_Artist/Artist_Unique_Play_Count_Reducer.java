/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package zOther.Filtered_.A3_Unique_Play_Count_Per_Artist;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Artist_Unique_Play_Count_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalUniquePlayCount = 0;

        // get count and add
        for (IntWritable uniqueCount : values) {
            totalUniquePlayCount += uniqueCount.get();
        }

        result.set(totalUniquePlayCount);

        // print artists name with unique count
        context.write(key, result);
    }
}

