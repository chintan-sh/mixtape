/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Summarized_.A4_User_Gender_Count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class User_Gender_Count_Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text gender;
    private IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get user info
        String[] userInfo = value.toString().split("\t");
        String userGender = userInfo[1];

        // extract user gender
        gender = new Text(userGender);

        context.write(gender, one);
    }
}
