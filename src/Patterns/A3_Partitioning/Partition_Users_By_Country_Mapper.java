/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns.A3_Partitioning;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 *
 * @author Chintan
 */
public class Partition_Users_By_Country_Mapper extends Mapper<Object, Text, Text, Text> {
    private Text country = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get user info
        String[] userInfo = value.toString().split("\t");
        String userCountryGender = userInfo[3].trim();

        country.set(userCountryGender);
        context.write(country, value);
    }
}
