/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns.A7_CountingWithCounters;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Locale;

/**
 *
 * @author Chintan
 */
public class Most_Listens_By_Time_of_Day_Mapper extends Mapper<Object, Text, NullWritable, NullWritable> {
    public static final String HOUR_COUNTER_GROUP = "Hour";
    public static final String NULL_OR_EMPTY = "Null";

    // declare arr of hrs
    private String[] hourArr = new String[]{
       "0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23"
    };

    // init hashset with hrs as keys
    private HashSet<String> hours = new HashSet<String>(Arrays.asList(hourArr));

    private IntWritable createHour = new IntWritable(); // 2009-04-08T01:57:47Z
    private final static SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US); //"yyyy-MM-dd'T'HH:mm:ssZ"


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get user info
        String[] userInfo = value.toString().split("\t");
        String playTime = userInfo[1].trim();

        Calendar cal = Calendar.getInstance();
        try {
            if(playTime.equals("")){
                context.getCounter(HOUR_COUNTER_GROUP, NULL_OR_EMPTY).increment(1);
            }else {
                cal.setTime(fmt.parse(playTime));
                createHour.set(cal.get(Calendar.HOUR_OF_DAY));
                if (hours.contains(createHour.toString())) {
                    context.getCounter(HOUR_COUNTER_GROUP, createHour.toString()).increment(1);
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
