/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A6_User_Differentiation_By_Age;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author Chintan
 */
public class Partition_Users_By_Age_Partitioner extends Partitioner<IntWritable, Text>{

    @Override
    public int getPartition(IntWritable key, Text value, int numOfPartitions) {
        if(numOfPartitions == 0)
            return 0;
        if(key.get() >= 11 && key.get() <= 17)
            return 1;
        if(key.get() >= 18 && key.get() <= 25)
            return 2;
        if(key.get() >= 26 && key.get() <= 35)
            return 3;
        if(key.get() >= 36 && key.get() <= 49)
            return 4;
        if(key.get() >= 50 && key.get() <= 65)
            return 5;
        if(key.get() >= 66 && key.get() <= 80)
            return 6;
        if(key.get() >= 81 && key.get() <= 99)
            return 7;
        else
            return 0;
    }
}

//11-17, 18-25, 26-35, 36-49,50-65,66-80, 81-99