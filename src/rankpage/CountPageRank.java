/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rankpage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
        private List<String> followedByUserId; // list user followed by userId
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer token = new StringTokenizer(value.toString());
            userId.set(token.nextToken());
            twitterRankUserId = new Double(token.nextToken());
            following.set(token.nextToken());
            followedByUserId = new ArrayList<String>(Arrays.asList(following.toString().split(",")));
            
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
            String following = "";
            Double newTwitterRank = new Double(0);
            int n = 0;
            for(User user : users){
                newTwitterRank += user.getPageRank();
                if(!following.equals("") && !user.getFolowing().equals(new Text())){
                    following += ",";
                }
                following += user.getFolowing().toString();
                n++;
            }
            if(following.equals("")) following = ",";
            newTwitterRank = (1 - D) + (D * newTwitterRank);
            result.set(newTwitterRank, new Text(following));
            context.write(key, result);
        }
        
    }
}
