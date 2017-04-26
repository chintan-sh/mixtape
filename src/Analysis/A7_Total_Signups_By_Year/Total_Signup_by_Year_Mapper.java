/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A7_Total_Signups_By_Year;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Total_Signup_by_Year_Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text year;
    private IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get user info
        String[] userInfo = value.toString().split("\t");
        String signupDate = userInfo[4].trim();
        String signupYear = signupDate.split(",")[1];

        // extract signup year
        year = new Text(signupYear);

        context.write(year, one);
    }
}
