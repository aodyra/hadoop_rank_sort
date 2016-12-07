/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rankpage;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author aodyra
 */
public class RankPage {
    
    private static Configuration conf = new Configuration();
    private static int numReduce = 122;

    public static void deleteFolder(String path) throws Exception {
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(path), true);
    }
    
    public static void preprocess(String input) throws Exception{
        String output = "/user/aodyra/output_preprocess";
        deleteFolder(output);
        Job job = Job.getInstance(conf, "(aodyra) preprocess");
        job.setJarByClass(PreProcess.class);
        job.setMapperClass(PreProcess.UserMapper.class);
        job.setCombinerClass(PreProcess.UserReducer.class);
        job.setReducerClass(PreProcess.UserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(User.class);
        job.setNumReduceTasks(numReduce);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static void iterate(int count) throws Exception{
        String input = "";
        if(count == 1){
            input = "/user/aodyra/output_preprocess";
        } else {
            input = "/user/aodyra/output_iterate" + (count - 1);
        }
        String output = "/user/aodyra/output_iterate" + count;
        deleteFolder(output);
        Job job = Job.getInstance(conf, "(aodyra) iterate"+count);
        job.setJarByClass(CountPageRank.class);
        job.setMapperClass(CountPageRank.RankMapper.class);
        job.setReducerClass(CountPageRank.RankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(User.class);
        job.setNumReduceTasks(numReduce);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
    
    public static void sortRank() throws Exception{
        String input = "/user/aodyra/output_iterate3";
        String output = "/user/aodyra/output_sort";
        deleteFolder(output);
        Job job = Job.getInstance(conf, "(aodyra) sortrank ezpz");
        job.setJarByClass(SortPageRank.class);
        job.setMapperClass(SortPageRank.SortRankMapper.class);
        job.setReducerClass(SortPageRank.SortRankReducer.class);
//        job.setCombinerClass(SortPageRank.SortRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(User.class);
        job.setNumReduceTasks(numReduce);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.waitForCompletion(true);
    }
    
    public static void doIterate(int iterate) throws Exception{
        for(int i = 1; i <= iterate; ++i){
            iterate(i);
        }
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        String input_data = args[0];
        try {
            sortRank();
        } catch (Exception x) {
            Logger.getLogger(RankPage.class.getName()).log(Level.SEVERE, null, x);
        }
    }
    
}
