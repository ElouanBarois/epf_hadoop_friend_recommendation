package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelationshipMapper extends Mapper<LongWritable, Relationship, Text, Text> {

    @Override
    protected void map(LongWritable key, Relationship value, Context context) throws IOException, InterruptedException {
        String id1 = value.getId1();
        String id2 = value.getId2();

        context.write(new Text(id1), new Text(id2));
        context.write(new Text(id2), new Text(id1));
    }
}
