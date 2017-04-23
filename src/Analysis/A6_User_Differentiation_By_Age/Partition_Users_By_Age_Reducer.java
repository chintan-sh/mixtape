/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A6_User_Differentiation_By_Age;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Partition_Users_By_Age_Reducer extends Reducer<IntWritable, Text, Text, NullWritable > {
    private MultipleOutputs<Text, NullWritable> multipleOutputs;

    public void setup(Mapper.Context context){
        multipleOutputs = new MultipleOutputs(context);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
                multipleOutputs.write("ageBins", value, NullWritable.get(), key.toString());
        }
    }

    public void cleanup(Mapper.Context context) throws IOException, InterruptedException{
        multipleOutputs.close();
    }
}
