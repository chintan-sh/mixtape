/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A6_User_Differentiation_By_Age;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Partition_Users_By_Age_Mapper extends Mapper<Object, Text, IntWritable, Text> {
    private IntWritable age = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get user info
        String[] userInfo = value.toString().split("\t");
        String userAge = userInfo[2].trim();

        if (!userAge.equals("")) {
            age.set(Integer.parseInt(userAge));
            context.write(age, value);
        }
    }
}
