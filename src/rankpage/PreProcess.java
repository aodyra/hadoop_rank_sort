/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rankpage;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author aodyra
 */
public class PreProcess {
    public static class UserMapper extends Mapper<LongWritable, Text, Text, User>{
        private User result = new User();
        private Text user_id = new Text();
        private Text following_id = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer token = new StringTokenizer(value.toString());
            
            user_id.set(token.nextToken());
            following_id.set(token.nextToken());
            
            result.set(new Double(1), new Text());
            context.write(user_id, result);
            
            result.set(new Double(1), user_id);
            context.write(following_id, result);
        }
    }
    
    public static class UserReducer extends Reducer<Text, User, Text, User>{
        private User result = new User();

        @Override
        protected void reduce(Text key, Iterable<User> users, Context context) throws IOException, InterruptedException {
            String following = "";
            for(User user : users){
                if(!following.equals("") && !user.getFolowing().equals(new Text())){
                    following += ",";
                }
                following += user.getFolowing().toString();
            }
            if(following.equals("")) following = ",";
            result.set(new Double(1), new Text(following));
            context.write(key, result);
        }
    }
}
