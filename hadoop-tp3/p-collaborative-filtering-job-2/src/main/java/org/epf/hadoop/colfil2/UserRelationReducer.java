package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class UserRelationReducer extends Reducer<UserPair, Text, UserPair, Text> {

    private Text result = new Text();

    @Override
    protected void reduce(UserPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueUsers = new HashSet<>();
        boolean directRelation = false;

        for (Text user : values) {
            if (uniqueUsers.contains(user.toString())) {
                directRelation = true;
                break;
            }
            uniqueUsers.add(user.toString());
        }

        if (!directRelation && uniqueUsers.size() > 1) {
            result.set(String.valueOf(uniqueUsers.size()));
            context.write(key, result);
        }
    }
}
