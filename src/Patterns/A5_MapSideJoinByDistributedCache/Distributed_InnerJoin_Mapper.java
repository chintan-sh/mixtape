/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Patterns.A5_MapSideJoinByDistributedCache;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;

/**
 *
 * @author Chintan
 */
public class Distributed_InnerJoin_Mapper extends Mapper<Object, Text, Text, Text> {
    private static HashMap<String, String> userBioMap = new HashMap<>();
    private BufferedReader lineReader;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        // if cache is not empty, fetch the file we uploaded
        if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
            File userfile = new File("./user");
            String singleLineRead;

            try {
                lineReader = new BufferedReader(new FileReader(userfile.toString()));

                // Read each line, split and load to HashMap
                while ((singleLineRead = lineReader.readLine()) != null) {
                    String userFieldArray[] = singleLineRead.split("\t");

                    // check if other data is present else ignore
                    if(userFieldArray.length > 1) {
                        // if yes
                        if (!userFieldArray[0].trim().equals("") && !userFieldArray[1].trim().equals("")) {
                            // extract userid from data (as it would be present during join and attach other data)
                            String restOfValue = singleLineRead.substring(singleLineRead.indexOf("\t") + 1);

                            // put into Map
                            userBioMap.put(userFieldArray[0].trim(), restOfValue);
                        }
                    }
                }

            } catch (FileNotFoundException e) { e.printStackTrace(); } catch (IOException e) {
                e.printStackTrace();
            }finally {
                if (lineReader != null) { lineReader.close();}
            }
        }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] artistInfo = value.toString().split("\t");
        String userId = artistInfo[0];

        // check if current fellow is one of the folks in hashmap, if yes - Let 'em in!
        if(userBioMap.containsKey(userId)){
            // Now join this guy and guy in hashmap
            String joinVal = value.toString().substring(value.toString().indexOf("\t") + 1) + "\t" + userBioMap.get(userId);
            context.write(new Text(userId), new Text(joinVal));
        }
    }
}
