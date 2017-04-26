/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A8_Top_10_Most_Popular_Tracks;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Top_10_Most_Popular_Tracks_Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text trackName;
    private IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get artist info
        String[] trackInfo = value.toString().split("\t");

        String tName = trackInfo[5].trim();

        if(!tName.equals("[Untitled]") && !tName.equals("Untitled")) {
            // extract artist name
            trackName = new Text(tName);
            context.write(trackName, one);
        }
    }
}
