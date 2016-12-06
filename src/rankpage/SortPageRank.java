/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rankpage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author aodyra
 */
public class SortPageRank {
    public static List<User> sortRank = new ArrayList<User>();
    
    public static class SortRankMapper extends Mapper<LongWritable, Text, Text, User>{
        private User result = new User();
        private Text userId = new Text();
        private Double userIdPageRank;
        private Text following = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer token = new StringTokenizer(value.toString());
            userId.set(token.nextToken());
            userIdPageRank = new Double(token.nextToken());
            following.set(token.nextToken());
            result.set(userId, userIdPageRank, following);
            context.write(new Text("-1"), result);
        }
        
    }
    
    public static class SortRankReducer extends Reducer<Text, User, Text, User>{
        @Override
        protected void reduce(Text key, Iterable<User> users, Context context) throws IOException, InterruptedException {
            for(User user : users){
                if (sortRank.size() < 5){
                    sortRank.add(user);
                    if(sortRank.size() == 5){
                        Collections.sort(sortRank);
                    }
                } else {
                    if(user.compareTo(sortRank.get(0)) > 0){
                        sortRank.set(0, user);
                        Collections.sort(sortRank);
                    }
                }
            }
            for(int i = sortRank.size()-1; i >= 0 ; --i){
                context.write(sortRank.get(i).getUserId(), sortRank.get(i));
            }
        }
    }
}
