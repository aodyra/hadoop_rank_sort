/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rankpage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author aodyra
 */
public class CountPageRank {
    public static Double D = 0.85;
    public static class RankMapper extends Mapper<LongWritable, Text, Text, User>{
        private User result = new User();
        private Text userId = new Text(); // userId following
        private Double twitterRankUserId;
        private Text following = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer token = new StringTokenizer(value.toString());
            userId.set(token.nextToken());
            twitterRankUserId = new Double(token.nextToken());
            following.set(token.nextToken());
            ArrayList<String> followedByUserId = new ArrayList<String>(Arrays.asList(following.toString().split(",")));
            
            result.set(new Double(0), following);
            context.write(userId, result);
            
            double pageRankFromUserId = twitterRankUserId / followedByUserId.size();
            for(int i = 0; i < followedByUserId.size(); ++i){
                result.set(pageRankFromUserId, new Text());
                context.write(new Text(followedByUserId.get(i)), result);
            }
        }
    }
    
    public static class RankReducer extends Reducer<Text, User, Text, User>{
        private User result = new User();
        @Override
        protected void reduce(Text key, Iterable<User> users, Context context) throws IOException, InterruptedException {
            Double newTwitterRank = new Double(0);
            for(User user : users){
                newTwitterRank += user.getPageRank();
                if(!user.getFollowing().toString().equals("")){
                    String following = user.getFollowing().toString();
                    result = new User(newTwitterRank, following);
                }
            }
            newTwitterRank = (1 - D) + (D * newTwitterRank);
            if(!result.getFollowing().toString().equals("")){
                result = new User(newTwitterRank, result.getFollowing().toString());
                context.write(key, result);
            }
        }
        
    }
}
