/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package zOther.Filtered_.A4_User_Traffic_By_Country;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Total_Traffic_by_Country_Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text country;
    private IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get user info
        String[] userInfo = value.toString().split("\t");
        String cID = userInfo[3];

        // extract artist name
        country = new Text(cID);

        context.write(country, one);
    }
}
