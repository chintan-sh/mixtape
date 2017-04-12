/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Total_Arist_Play_Count;

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

// Run this command on Linux CLI once results are computed => cat part-r-00000 | sed 's/.*/\L&/; s/[a-z]*/\u&/g' | sort -t$'\t' -n -k2 -r | head -n 20