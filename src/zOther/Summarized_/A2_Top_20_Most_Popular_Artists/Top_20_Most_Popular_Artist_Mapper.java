/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package zOther.Summarized_.A2_Top_20_Most_Popular_Artists;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class Top_20_Most_Popular_Artist_Mapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text artistName;
    private IntWritable one = new IntWritable(1);

    // incoming input : user-mboxsha1 \t musicbrainz-artist-id \t artist-name \t plays
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get artist info
        String[] artistInfo = value.toString().split("\t");

        // cleanup artist name by removing all whitespaces
        String aName = artistInfo[2] + "\t"; //.replaceAll("\\s+","");

        // extract artist name
        artistName = new Text(aName);

        context.write(artistName, one);
    }
}
