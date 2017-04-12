/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package A2_Sum_Play_Count_Per_Artist;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Chintan
 */
public class Artist_Play_Count_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalPlayCount = 0;

        // get playcount and add
        for (IntWritable playCount : values) {
            totalPlayCount += playCount.get();
        }

        result.set(totalPlayCount);

        // print artists name with playcount
        context.write(key, result);
    }
}
