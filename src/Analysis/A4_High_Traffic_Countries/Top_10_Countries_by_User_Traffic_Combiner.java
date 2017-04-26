/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A4_High_Traffic_Countries;

import org.apache.hadoop.io.IntWritable;
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
public class Top_10_Countries_by_User_Traffic_Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    public IntWritable total = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for( IntWritable value : values) {
            count += value.get();
        }

        total.set(count);
        context.write(key, total);

    }

}

