/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package zOther.ZExtras_.A5_Top_20_Traffic_By_Country;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author pooja
 */
public class Top_20_Traffic_SortComparator extends WritableComparator {

    protected Top_20_Traffic_SortComparator(){
        super(FloatWritable.class, true);
    }
    
    @Override
    public int compare(WritableComparable w1, WritableComparable w2)
    {
        FloatWritable key1 = (FloatWritable) w1;
        FloatWritable key2 = (FloatWritable) w2;
        
           return  -1 * key1.compareTo(key2) ;// (cw1.getDeptNo().compareTo(cw2.getDeptNo()));
    }
}
