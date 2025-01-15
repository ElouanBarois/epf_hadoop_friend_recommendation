package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RecommendationMapper extends Mapper<LongWritable, Text, Text, RecommendationUser> {
    private Text outputKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split("\t");
        if (parts.length != 2) return;

        String[] users = parts[0].split("\\.");
        if (users.length != 2) return;


        int sameFriends = Integer.parseInt(parts[1]);

        outputKey.set(users[0]);
        context.write(outputKey, new RecommendationUser(
                users[0], users[1], sameFriends));

        outputKey.set(users[1]);
        context.write(outputKey, new RecommendationUser(
                users[1], users[0], sameFriends));
    }
}