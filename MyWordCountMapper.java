package com.mr06;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyWordCountMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

    Text outputValue = new Text();
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line = value.toString(); 
		String[] split = line.split("#");	
		String dd = split[10];
		String[] split1 = dd.split("/");
		double fz = Double.parseDouble(split1[0]);
		double fm = Double.parseDouble(split1[1]);
		if (split[2].length()==0){
			split[2]="0";
		};
		if (split[3].length()==0){
			split[3]="0";
		};
		double p=0;
		double duicuo = fz/fm;
		String j="000";
		String beginDate=split[15]+j;  		  
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");  		  
		String sd = sdf.format(new Date(Long.parseLong(beginDate)));  
		if(duicuo==0||duicuo==1) {
			 p = parameterTest(split);
		}
		else {
			if(split[11].equals("0")) {
				p = duicuo+(1-duicuo)*0.2;	
			}
			if(split[11].equals("1")) {
				p = duicuo;	
			}
			if(split[11].equals("2")) {
				p = duicuo-(1-duicuo)*0.1;	
			}
		}
		  
	        BigDecimal b = new BigDecimal(p);
	        /*setScale 第一个参数为保留位数 第二个参数为舍入机制
	         BigDecimal.ROUND_DOWN 表示不进位 
	         BigDecimal.ROUND_UP表示进位*/
	        p = b.setScale(4, BigDecimal.ROUND_DOWN).doubleValue();

		String P = String.valueOf(p);
		
		int i=1;
		//int i1=10000000;
		int i2=1;
		//userId0 groupId1 countryId2 provinceId3  timu questionJsonPoint6 subject7 time timeStamp analysis i
		outputValue.set(split[0]+" "+split[1]+" "+split[2]+" "+split[3]+" "+split[4]+" "+split[6]+" "+split[7]+" "+P+" "+i+" "+i2+" "+sd+" "+split[15]+" "+duicuo);	    
		context.write(key, outputValue);
	}

	public static double parameterTest(String[] split) {
		String dd = split[10];
		String[] split1 = dd.split("/");
		double fz = Double.parseDouble(split1[0]);
		double fm = Double.parseDouble(split1[1]);
		String dt = split[5];
		String dn = split[11];
		String dz = split[16];
		double duicuo = 1-fz/fm;
	
		double tixing = Double.parseDouble(dt);
		double nanyidu = Double.parseDouble(dn);
		double zouye = Double.parseDouble(dz);
		double chanshu1 = 0;
		double chanshu2 = 0;
		double chanshu3 = 0;
		double chanshu4 = 0;
		if(zouye==0){
			if(tixing==0 && duicuo==0.0){
				chanshu1 = 0.8;
				chanshu3 = 0.05;
 			}
			if(tixing==0 && duicuo==1.0){
				chanshu1 = 0.3;
				chanshu4 = 0.225;
			}
			if(tixing==1 && duicuo==0.0){
				chanshu1 = 0.7;
				chanshu3 =0.02;
			}
			if(tixing==1 && duicuo==1.0){
				chanshu1 = 0.25;
				chanshu4 = 0.23;
			}
			if(tixing==2 && duicuo==0.0){
				chanshu1 = 0.6;
				chanshu3 = 0.2;
			}
			if(tixing==2 && duicuo==1.0){
				chanshu1 = 0.15;
				chanshu4 = 0.075;
			}
			if(tixing!=1 && tixing!=2 && duicuo==0.0){
				chanshu1 = 0.7;
				chanshu3 =0.02;
			}
			if(tixing!=1 && tixing!=2 && duicuo==1.0){
				chanshu1 = 0.25;
				chanshu4 = 0.23;
			}
			
		}
		if(zouye==1){
			if(tixing==0 && duicuo==0.0){
				chanshu1 = 0.75;
				chanshu3 = 0.0625;
 			}
			if(tixing==0 && duicuo==1.0){
				chanshu1 = 0.25;
				chanshu4 = 0.1875;
			}
			if(tixing==1 && duicuo==0.0){
				chanshu1 = 0.65;
				chanshu3 =0.0233;
			}
			if(tixing==1 && duicuo==1.0){
				chanshu1 = 0.2;
				chanshu4 = 0.214;
			}
			if(tixing==2 && duicuo==0.0){
				chanshu1 = 0.55;
				chanshu3 = 0.225;
			}
			if(tixing==2 && duicuo==1.0){
				chanshu1 = 0.1;
				chanshu4 = 0.05;
			}
			if(tixing!=1 && tixing!=2 && duicuo==0.0){
				chanshu1 = 0.65;
				chanshu3 =0.0233;
			}
			if(tixing!=1 && tixing!=2 && duicuo==1.0){
				chanshu1 = 0.2;
				chanshu4 = 0.214;
			}
		}
		double p=0;
		if(duicuo==0){
			double i =chanshu1*(1-chanshu4);
		
			double j =chanshu1*(1-chanshu4)+(1-chanshu1)*chanshu3;
			
			p=i/j;
		}
		else{
			double i =chanshu1*chanshu4;
		
			double j =chanshu1*chanshu4+(1-chanshu1)*(1-chanshu3);
			p=i/j;
		}
		return p;
	}
	
}


