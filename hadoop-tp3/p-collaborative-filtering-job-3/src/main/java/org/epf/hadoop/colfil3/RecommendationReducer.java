package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class RecommendationReducer extends Reducer<Text, RecommendationUser, Text, Text> {
    private static final int max_n = 5;
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<RecommendationUser> values, Context context)
            throws IOException, InterruptedException {
        TreeSet<RecommendationUser> bestRec = new TreeSet<>((a, b) -> {
            int compare = -Integer.compare(a.getSameFriends(), b.getSameFriends());
            if (compare != 0) return compare;

            return a.getRecUserId().compareTo(b.getRecUserId());
        });

        for (RecommendationUser val : values) {
            RecommendationUser copy = new RecommendationUser(
                    val.getUserId(),
                    val.getRecUserId(),
                    val.getSameFriends()
            );
            bestRec.add(copy);

            if (bestRec.size() > max_n) {
                bestRec.pollLast();
            }
        }

        if (!bestRec.isEmpty()) {
            List<String> rec2 = new ArrayList<>();
            for (RecommendationUser rec : bestRec) {
                rec2.add(String.format("%s(%d)",
                        rec.getRecUserId(), rec.getSameFriends()));
            }
            result.set(String.join(", ", rec2));
            context.write(key, result);
        }
        if (bestRec.isEmpty()) {
            result.set("No recommendations");
            context.write(key, result);
        }
    }
}