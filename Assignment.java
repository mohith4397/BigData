import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Assignment {
	public static class AssignmentMapper extends Mapper<LongWritable,Text,Text,Text>
	{
	public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException
	{
		String str[]=values.toString().split(","); 
		String age= str[0].replaceAll("[^0-9]", "");
		String Income=str[5].replace("\"Income\":  ","");
		String ageclass=ageclass(Integer.parseInt(age));
		context.write(new Text(ageclass), new Text(Income));
	
	}
	String ageclass(int age){
		if(age<10)
	         return "infants";
	      else if(age>=10 && age<=18)
	         return "Teenager" ;
	      else if(age>18 && age <=40)
	         return "adult";
	      else if(age>40 && age <=60)
	    	  return "middle-aged";
	      else if(age>60 && age <=80)
	    	  return "senior citizen";
	      else if(age>80 && age <=100)
	    	  return "elderly";
		return null;
	}
	}
	
	
	public static class AssignmentReducer extends Reducer<Text,Text,Text,Text>	
	{
		public void reduce(Text key,Iterable <Text> values,Context context) throws InterruptedException, IOException
		
		{ 
			int count=0; float sum=0;
			for(Text val : values)
			{
			float income=Float.parseFloat(val.toString());
			sum=sum+income;
			++count;
			}
			float average = sum/count;
			context.write(key,new Text(Float.toString(average)));
			
		}
		
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Assignment");
	    job.setJarByClass(Assignment.class);
	    job.setMapperClass(AssignmentMapper.class);
	    job.setReducerClass(AssignmentReducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	
}
