/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Analysis.A10_Weekday_v_Weekend_Listens;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 *
 * @author Chintan
 */
public class Listen_History_Weekday_Weekend_Mapper extends Mapper<Object, Text, NullWritable, NullWritable> {
    public static final String DAY_COUNTER_GROUP = "Day";
    public static final String NULL_OR_EMPTY = "Null";

    // declare arr of hrs
    private String[] dayArr = new String[]{
       "Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"
    };

    // init hashset with hrs as keys
    private HashSet<String> days = new HashSet<String>(Arrays.asList(dayArr));

//    private Text createDay = new Text(); // 2009-04-08T01:57:47Z
//    private final static SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); //"yyyy-MM-dd'T'HH:mm:ssZ"


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // get user info
        String[] userInfo = value.toString().split("\t");
        String playTime = userInfo[1].trim();

        try {

            if(playTime.equals("")){
                context.getCounter(DAY_COUNTER_GROUP, NULL_OR_EMPTY).increment(1);
            }else {
                Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(playTime);

                // Then get the day of week from the Date based on specific locale.
                String dayOfWeek = new SimpleDateFormat("EEEE", Locale.ENGLISH).format(date);

                //System.out.println(dayOfWeek); // Friday
                if (days.contains(dayOfWeek)) {
                    context.getCounter(DAY_COUNTER_GROUP, dayOfWeek).increment(1);
                }
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

}






//      Calendar cal = Calendar.getInstance();
//        DateFormatSymbols dfs = new DateFormatSymbols(Locale.US);
//        String weekdays[] = dfs.getWeekdays();
//
//
//      try {
//            if(playTime.equals("")){
//                context.getCounter(DAY_COUNTER_GROUP, NULL_OR_EMPTY).increment(1);
//            }else {
//                System.out.println("Parsed " + fmt.parse(playTime).getTime());
//                cal.setTime(fmt.parse(playTime));
//                System.out.println(" Weekday found '" + cal.DATE + "'");
//                createDay.set(weekdays[Calendar.DAY_OF_WEEK]);
//                //System.out.println(" Day found '" + createDay + "'");
//                if (days.contains(createDay.toString())) {
//                    context.getCounter(DAY_COUNTER_GROUP, createDay.toString()).increment(1);
//                }
//            }
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }