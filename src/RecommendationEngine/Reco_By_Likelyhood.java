package RecommendationEngine;

/**
 * Created by chintan on 4/24/17.
 */

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.commons.csv.CSVParser;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.MemoryIDMigrator;
import org.apache.mahout.cf.taste.impl.recommender.GenericBooleanPrefItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;



public class Reco_By_Likelyhood {
    private MemoryIDMigrator stringToLong = new MemoryIDMigrator();
    private static String DATA_FILE_NAME = "/home/chintan/IdeaProjects/AdvancedDBMS/music-project/inputTest/data5.csv";
    private Recommender recommender = null;
    private static DataModel dataModel;
    public String[] line;

    public void initialize() {
        try {
            // create hashmap to store users, play count
            Map<Long,List<Preference>> playcountOfUsers = new HashMap<>();

            // a CSV parser for reading the file
            CSVParser parser = new CSVParser(new InputStreamReader(new FileInputStream(DATA_FILE_NAME), "UTF-8"));

            // go through every line
            while((line = parser.getLine()) != null) {
                System.out.println(line[0] + "," +line[1]);
                // Extract user, artist and playcount
                String user = line[0];
                String artistName = line[1];
                float playCount = Float.parseFloat(line[2]);

                // store the mapping for the user
                long userLong = stringToLong.toLongID(user);
                stringToLong.storeMapping(userLong, user);

                // store the mapping for the artist
                long artistLong = stringToLong.toLongID(artistName);
                stringToLong.storeMapping(artistLong, artistName);

                // DS_store for single user's several playcounts
                List<Preference> userPlayCountList;

                // init user playcount list
                if((userPlayCountList = playcountOfUsers.get(userLong)) == null) {
                    userPlayCountList = new ArrayList<>();
                    playcountOfUsers.put(userLong, userPlayCountList);
                }
                // add the similarities found to this user
                userPlayCountList.add(new GenericPreference(userLong, artistLong, playCount)); //1
            }

            // create the corresponding mahout data structure from the map -> contains - userID, userPrefArr
            FastByIDMap<PreferenceArray> playcountOfUsersFastMap = new FastByIDMap<>();
            for(Entry<Long, List<Preference>> entry : playcountOfUsers.entrySet()) {
                playcountOfUsersFastMap.put(entry.getKey(), new GenericUserPreferenceArray(entry.getValue()));
            }

            // create a data model based on FastMap
            dataModel = new GenericDataModel(playcountOfUsersFastMap);

            // Recommender Instantiation
            recommender = new GenericBooleanPrefItemBasedRecommender(dataModel, new LogLikelihoodSimilarity(dataModel));

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("Hello : " +  line[0] + " how are you : " + line[1] + " ji : " + line[2]);
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String[] getRecommendationForUser(String personName) throws TasteException {
        List<String> recommendations = new ArrayList<>();
        try {
            // provide 5 recos
            List<RecommendedItem> items = recommender.recommend(stringToLong.toLongID(personName), 5);

            // put it on a list
            for(RecommendedItem item : items) {
                recommendations.add(stringToLong.toStringID(item.getItemID()));
            }
        } catch (TasteException e) {
            throw e;
        }

        // convert list into array and return
        return recommendations.toArray(new String[recommendations.size()]);
    }

    public static void main(String[] args) {
        Reco_By_Likelyhood artistReco = new Reco_By_Likelyhood();
        artistReco.initialize();
        try {
            System.out.println("\nRecommended artists for you : ");
            for (String outcome : artistReco.getRecommendationForUser("00a234b6ef00479de0eeb11c07682c426da65dcb")) { //009b9bfaa9c8e382b77ede26f347071572438986 //008f7a869d7e53e315d0c010348512b4161aed34
                System.out.println(outcome.substring(0, 1).toUpperCase() + outcome.substring(1));
            }
        } catch (TasteException e) {
            e.printStackTrace();
        }
    }
}
