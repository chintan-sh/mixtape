/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package zOther.ZExtras_.A5_Top_20_Traffic_By_Country;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Top_20_Traffic_Reducer_1 extends Reducer<Text, IntWritable, Text, IntWritable> {
    private  int count = 0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for( IntWritable value : values) {
                count = count + 1;
            }

            context.write(key, new IntWritable(count));
    }

}

