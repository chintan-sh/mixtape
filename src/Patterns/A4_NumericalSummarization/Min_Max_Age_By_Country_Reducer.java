/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns.A4_NumericalSummarization;

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
            //System.out.println("List size found for " + key + " is " + list.size() + " and counting observed as " + running_count);
            if(list.size() % 2 == 0){
                //System.out.println("Count even : " + list.size());
                median = (list.get((list.size()/2)-1) +  list.get((list.size()/2)))/2;
            }else{
                //System.out.println("Count odd : " + list.size());
                //System.out.println("Element to be selected : " + list.size()/2);
                median = list.get((list.size()/2));
                //System.out.println("Median calculated : " + median);
            }

            // calculating mean
            //float average = running_sum/list.size();

            // form an output "\t" + String.valueOf(average)
            String op = String.valueOf(minAge)  + "\t" + String.valueOf(median)+ "\t" + String.valueOf(maxAge);
            Text out = new Text(op);

            // output country with min, avg, median and max age
            context.write(key, out);

        }
}
