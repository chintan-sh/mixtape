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
public class InnerJoin_User_Mapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] userInfo =  value.toString().split("\t");
        String userId = userInfo[0];

        //append 'A' for user identification
        String user = "A" + userInfo[1] + userInfo[2] + userInfo[3] + userInfo[4]  ;

        // userID, user
        context.write(new Text(userId), new Text(user));
    }
}
