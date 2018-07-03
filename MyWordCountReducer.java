package com.mr06;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

      
/**
 * map���������������reduce����������
 * 
 * Text, IntWritable��ǰ������reduce��������������
 * Text, IntWritable�� ������,reduce�������������
 * @author Administrator
 *
 */
public class MyWordCountReducer extends
Reducer<LongWritable, Text, Text, NullWritable> {
protected void reduce(
    LongWritable k2,
    java.lang.Iterable<Text> v2s,
    org.apache.hadoop.mapreduce.Reducer<LongWritable, Text, Text, NullWritable>.Context context)
    throws java.io.IOException, InterruptedException {
for (Text v2 : v2s) {
    context.write(v2, NullWritable.get());
}
};
}