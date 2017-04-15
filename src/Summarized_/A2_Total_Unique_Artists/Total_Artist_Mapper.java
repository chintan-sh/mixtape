/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Summarized_.A2_Total_Unique_Artists;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Total_Artist_Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text artistName;
    private IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get artist info
        String[] artistInfo = value.toString().split("\t");

        // cleanup artist name by removing all whitespaces
        String aName = artistInfo[2];

        // extract artist name
        artistName = new Text(aName);

        context.write(artistName, one);
    }
}
