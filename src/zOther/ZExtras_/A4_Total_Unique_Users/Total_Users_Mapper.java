/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package zOther.ZExtras_.A4_Total_Unique_Users;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Total_Users_Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text userID;
    private IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get user info
        String[] userInfo = value.toString().split("\t");
        String uID = userInfo[0];

        // extract artist name
        userID = new Text(uID);

        context.write(userID, one);
    }
}
