package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecommendationUser implements WritableComparable<RecommendationUser> {
    private String userId;
    private String recUserId;
    private int sameFriends;

    public RecommendationUser() {
        this.userId = "";
        this.recUserId = "";
        this.sameFriends = 0;
    }

    public RecommendationUser(String userId, String recUserId, int sameFriends) {
        this.userId = userId;
        this.recUserId = recUserId;
        this.sameFriends = sameFriends;
    }

    public String getUserId() { return userId; }
    public String getRecUserId() { return recUserId; }
    public int getSameFriends() { return sameFriends; }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readUTF();
        recUserId = in.readUTF();
        sameFriends = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId);
        out.writeUTF(recUserId);
        out.writeInt(sameFriends);
    }


    @Override
    public int compareTo(RecommendationUser o) {
        int compare = -Integer.compare(this.sameFriends, o.sameFriends);
        if (compare != 0) return compare;
        return this.recUserId.compareTo(o.recUserId);
    }

@Override
    public String toString() {
        return recUserId + "(" + sameFriends + ")";
    }
}