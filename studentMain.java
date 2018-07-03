package com.mr06;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
// ��дjob��������
public class studentMain {
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			//conf.addResource("core-site.xml");
			//conf.addResource("hdfs-site.xml");
			//conf.addResource("mapred-site.xml");
			//conf.addResource("yarn-site.xml");
			// ָ��mapreduce���й�����ʹ�õ�jar�����ڵ�·��.��֤�˴���д�ڻ��job����֮ǰ
			//conf.set("mapreduce.job.jar", "f:\\wordcount.jar");
			FileSystem fs = FileSystem.get(conf);
			// ��õ�ǰ���������Ӧ��job����
			Job job = Job.getInstance(conf);
			
			job.setJarByClass(studentMain.class);
			
			Date dNow = new Date();   //当前时间
			Date dBefore = new Date();

			Calendar calendar = Calendar.getInstance();  //得到日历
			calendar.setTime(dNow);//把当前时间赋给日历
			calendar.add(Calendar.DAY_OF_MONTH, -1);  //设置为前一天
			dBefore = calendar.getTime();   //得到前一天的时间

			SimpleDateFormat sdf=new SimpleDateFormat("yy-MM-dd"); //设置时间格式
			String defaultStartDate = sdf.format(dBefore);    //格式化前一天
			defaultStartDate = "18-06-30";
			Path srcPath1 = new Path("/data/tcpStudentHui/"+defaultStartDate+"/"+"*");//hello word abc ddd
			//Path srcPath1 = new Path("/data/tcpStudentHui/18-05-10/18-05-10.1525937347564.tmp");
			
			MultipleInputs.addInputPath(job, srcPath1, TextInputFormat.class);
		
			job.setOutputFormatClass(TextOutputFormat.class);
			//设置保存路径
          //  Date date = new Date();  
           // SimpleDateFormat sf = new SimpleDateFormat("yyyyMMdd");  
           // String nowDate = sf.format(date); 
			Path resultPath = new Path("/data/Stresult/");
			
			boolean b = fs.exists(resultPath);
			if(b){
				fs.delete(resultPath, true);
			}
		
			// ����job��������hdfs֮�ϵı���·����ע�⣺һ��Ҫ��֤����·��������
			SequenceFileOutputFormat.setOutputPath(job, resultPath);
			
			// ����job��map�׶���ʹ�õ���
			job.setMapperClass(MyWordCountMapper.class);
			// ����job��reduce�׶���ʹ�õ���
			job.setReducerClass(MyWordCountReducer.class);
			
			
			//����map�������key������
			job.setMapOutputKeyClass(LongWritable.class);
			// ����map�������value������
			job.setMapOutputValueClass(Text.class);
			// ����reduce�����key������
			job.setOutputKeyClass(Text.class);
			//����redcue�������value������
			job.setOutputValueClass(NullWritable.class);
			// �ύjob�����ȴ�jobִ�����
			job.waitForCompletion(true);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
