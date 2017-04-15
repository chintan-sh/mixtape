/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Summarized_.A3_Top_20_Most_Played_Artists;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Top_20_Most_Played_Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text artistName;
    private IntWritable playCount;

    // incoming input : user-mboxsha1 \t musicbrainz-artist-id \t artist-name \t plays
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] artistInfo = value.toString().split("\t");


        String aName = artistInfo[2];

        // extract artist name
        artistName = new Text(aName);
        playCount = new IntWritable(Integer.parseInt(artistInfo[3].trim()));

        context.write(artistName, playCount);
    }
}
