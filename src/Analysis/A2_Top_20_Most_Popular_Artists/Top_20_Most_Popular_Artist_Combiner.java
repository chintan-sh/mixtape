/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A2_Top_20_Most_Popular_Artists;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author Chintan
 */
public class Top_20_Most_Popular_Artist_Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    public IntWritable total = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalUniquePlayCount = 0;

        // get count and add
        for (IntWritable uniqueCount : values) {
            totalUniquePlayCount += uniqueCount.get();
        }

        total.set(totalUniquePlayCount);
        context.write(key, total);
    }

}
