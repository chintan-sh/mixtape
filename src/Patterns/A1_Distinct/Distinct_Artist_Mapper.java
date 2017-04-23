/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns.A1_Distinct;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Distinct_Artist_Mapper extends Mapper<Object, Text, Text, NullWritable> {
    private Text artistName;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get artist info
        String[] artistInfo = value.toString().split("\t");

        // cleanup artist name by removing all whitespaces
        String aName = artistInfo[2] + "\t"; //.replaceAll("\\s+","");

        // extract artist name
        artistName = new Text(aName);

        context.write(artistName, NullWritable.get());
    }
}
