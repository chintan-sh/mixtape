/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns.A3_Partitioning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author Chintan
 */
public class Partition_Users_By_Country_Partitioner extends Partitioner<Text, Text>{

    @Override
    public int getPartition(Text key, Text value, int numOfPartitions) {
        if(numOfPartitions == 0)
            return 0;
        if(key.equals(new Text("United States")))
            return 1;
        if(key.equals(new Text("Germany")))
            return 2;
        if(key.equals(new Text("United Kingdom")))
            return 3;
        if(key.equals(new Text("Poland")))
            return 4;
        if(key.equals(new Text("Russian Federation")))
            return 5;
        if(key.equals(new Text("Brazil")))
            return 6;
        if(key.equals(new Text("Sweden")))
            return 7;
        if(key.equals(new Text("Spain")))
            return 8;
        if(key.equals(new Text("Finland")))
            return 9;
        if(key.equals(new Text("Netherlands")))
            return 10;
        else
            return 0;
    }
}

/*
    United States	67044
    Germany	31651
    United Kingdom	29902
    Poland	20987
    Russian Federation	19833
    Brazil	14534
    Sweden	13122
    Spain	13051
    Finland	11579
    Netherlands	9650
    Canada	8679
    France	7529
    Italy	7525
    Australia	7135
    Japan	6637
    Turkey	6452
    Norway	5155
    Mexico	4834
    Czech Republic	4774
    Ukraine	4396
*/