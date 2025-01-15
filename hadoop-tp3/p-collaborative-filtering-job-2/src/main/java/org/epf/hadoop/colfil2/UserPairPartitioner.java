package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class UserPairPartitioner extends Partitioner<UserPair, Text> {

    @Override
    public int getPartition(UserPair key, Text value, int numReduceTasks) {
        return Math.abs(key.getFirstUser().hashCode()) % numReduceTasks;
    }
}
