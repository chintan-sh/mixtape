/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Total_Arist_Play_Count;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Chintan
 */
public class Artist_Play_Count_Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text artistName;
    private IntWritable playCount;

    // incoming input : user-mboxsha1 \t musicbrainz-artist-id \t artist-name \t plays
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get artist info
        String[] artistInfo = value.toString().split("\t");

        // extract artist name
        artistName = new Text(artistInfo[2]);
        playCount = new IntWritable(Integer.parseInt(artistInfo[3].trim()));

        context.write(artistName, playCount);
    }
}
