/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns.A5_ReduceSideJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * @author Chintan
 */
public class InnerJoin_Artist_Mapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] artistInfo = value.toString().split("\t");
        String userId = artistInfo[0];

        //append 'T' for artist identification
        String artist = "T" + value.toString();

        // userID, artist
        context.write(new Text(userId), new Text(artist));
    }
}
