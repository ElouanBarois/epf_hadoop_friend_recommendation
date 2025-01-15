package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class UserRelationMapper extends Mapper<LongWritable, Text, UserPair, Text> {

    private UserPair userPair = new UserPair();
    private Text sharedContact = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length < 2) return;

        String user = parts[0];
        String[] contacts = parts[1].split(",");

        Arrays.sort(contacts);
        sharedContact.set(user);

        for (int i = 0; i < contacts.length; i++) {
            for (int j = i + 1; j < contacts.length; j++) {
                userPair = new UserPair(contacts[i], contacts[j]);
                context.write(userPair, sharedContact);
            }
        }
    }
}
