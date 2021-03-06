/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rankpage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author aodyra
 */
public class User implements Writable, Comparable<User>{
    private DoubleWritable pageRank;
    private Text following;
    
    public User(){
        this.pageRank = new DoubleWritable(1);
        this.following = new Text();
    }

    public User(Double pageRank, String following) {
        this.pageRank = new DoubleWritable(pageRank);
        this.following = new Text(following);
    }

    public Double getPageRank() {
        return pageRank.get();
    }

    public void setPageRank(Double pageRank) {
        this.pageRank.set(pageRank);
    }

    public Text getFollowing() {
        return following;
    }

    public void setFollowing(Text folowing) {
        this.following = following;
    }
    
    public void set(Double pageRank, Text following){
        this.pageRank.set(pageRank);
        this.following = following;
    }

    @Override
    public int compareTo(User other) {
        return this.getPageRank().compareTo(other.getPageRank());
    }
    
    @Override
    public void write(DataOutput d) throws IOException {
        pageRank.write(d);
        following.write(d);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        pageRank.readFields(di);
        following.readFields(di);
    }

    @Override
    public String toString() {
        return this.pageRank + "\t" + this.following.toString();
    }
    
}
