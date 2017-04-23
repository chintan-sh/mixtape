/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns.A5_ReduceSideJoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Chintan
 */
public class InnerJoin_User_Artist_Reducer extends Reducer<Text, Text, Text, Text> {

    private ArrayList<Text> listA = new ArrayList<>();
    private ArrayList<Text> listB = new ArrayList<>();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values){
            if(value.toString().charAt(0) == 'T' ){
                listA.add(new Text(value.toString().substring(1, value.toString().length()).trim() + "\t"));

            }else if(value.toString().charAt(0) == 'A' ){
                listB.add(new Text(value.toString().substring(1, value.toString().length()).trim()));
            }
        }


        if (!listB.isEmpty() && !listA.isEmpty()) {
            for (Text A : listA) {
                for (Text B : listB) {
                    try {
                        context.write(A, B);
                    } catch (IOException | InterruptedException ex) {
                        Logger.getLogger(InnerJoin_User_Artist_Reducer.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }

    }
}
