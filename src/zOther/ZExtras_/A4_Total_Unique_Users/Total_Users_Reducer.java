/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package zOther.ZExtras_.A4_Total_Unique_Users;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Total_Users_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static int count = 0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            count = count + 1;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Total Count => "), new IntWritable(count));
    }
}

