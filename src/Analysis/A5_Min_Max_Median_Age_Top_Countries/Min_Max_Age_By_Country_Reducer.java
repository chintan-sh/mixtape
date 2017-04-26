/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A5_Min_Max_Median_Age_Top_Countries;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 *
 * @author Chintan
 */
public class Min_Max_Age_By_Country_Reducer extends Reducer<Text, IntWritable, Text, Text> {
        private ArrayList<Integer> list = new ArrayList<>();
        private IntWritable one = new IntWritable(1);

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int minAge = 0;
            int maxAge = 0;
            int running_sum = 0;
            int running_count = 0;
            float median  = 0;

            for(IntWritable value : values){
                if(minAge <= 0 && value.get() > 10){
                    minAge = value.get();
                }

                if(value.get() > 10 && value.get() < minAge){
                    minAge = value.get();
                }

                if(value.get() < 80 && value.get() > maxAge){
                    maxAge = value.get();
                }

                list.add(value.get());
                running_sum += value.get();
                running_count += one.get();
            }

            Collections.sort(list);

            // calculating median
            if(list.size() % 2 == 0){
                median = (list.get((list.size()/2)-1) +  list.get((list.size()/2)))/2;
            }else{
                median = list.get((list.size()/2));
            }

            String op = String.valueOf(minAge)  + "\t" + String.valueOf(median)+ "\t" + String.valueOf(maxAge);
            Text out = new Text(op);

            context.write(key, out);

        }
}
