/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A5_Min_Max_Median_Age_Top_Countries;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Min_Max_Age_By_Country_Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text country = new Text();
    private IntWritable age = new IntWritable();
    public static int count = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get user info
        String[] userInfo = value.toString().split("\t");
        String userCountry = userInfo[0].trim();
        String userAge = userInfo[3].trim();


        if(!userAge.trim().equals("")) {
            country.set(userCountry);
            age.set(Integer.parseInt(userAge));

            context.write(country, age);
        }
//        else{
//            count++;
//            System.out.println("Total count " + count);
//        }
    }
}
