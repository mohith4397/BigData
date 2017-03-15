//3b Viable Product SubClass
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
public class ViableProductSubClass {
 public static class ViableProductSubClassMapper extends Mapper<LongWritable,Text,Text,Text>
 {
	protected void map(LongWritable Text,Text value,Context context) throws IOException, InterruptedException
	{
	String[] record=value.toString().split(";");
	String finalvalue=record[2]+","+record[8]+","+record[7];
	context.write(new Text(record[4]), new Text(finalvalue));
	}
 }
 public static class ViableProductSubClassPartitioner extends
 Partitioner < Text, Text >
 {
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks)
    {
  	  String[] str = value.toString().split(",");
	      if(str[0].trim().equals("A"))
	    	  return 0;
	      else if(str[0].trim().equals("B"))
	    	  return 1;
	      else if(str[0].trim().equals("C"))
	    	  return 2;
	      else if(str[0].trim().equals("D"))
	    	  return 3;
	      else if(str[0].trim().equals("E"))
	    	  return 4;
	      else if(str[0].trim().equals("F"))
	    	  return 5;
	      else if(str[0].trim().equals("G"))
	    	  return 6;
	      else if(str[0].trim().equals("H"))
	    	  return 7;
	      else if(str[0].trim().equals("I"))
	    	  return 8;
	      else if(str[0].trim().equals("J"))
	    	  return 9;
	      else if(str[0].trim().equals("K"))
	    	  return 10;
	      return 11;
	        
    }
    
 }

 public static class ViableProductSubClassReducer extends Reducer<Text,Text,NullWritable,Text>
 {
	 
	 private TreeMap<Double, Text> ViableProductSubClassMap = new TreeMap<Double, Text>();
	 
	 long SUMoftotalsale=0,SUMoftotalcost=0,SUMofquantity=0,profit=0;
	 String answer; double margin=0;
	 
	String Age=null;
	 
	 protected void reduce(Text key,Iterable <Text> value,Context context) throws IOException, InterruptedException
		{
		 for(Text val:value)
		 {
			 String[] record=val.toString().split(",");
			 SUMoftotalsale=SUMoftotalsale+Long.parseLong(record[1]);
			 SUMoftotalcost=SUMoftotalcost+Long.parseLong(record[2]);
			 Age=record[0];
		 }
		 profit=SUMoftotalsale-SUMoftotalcost;
		 margin=(profit*100)/SUMoftotalcost;
		 answer=key.toString()+","+String.valueOf(profit)+","+String.valueOf(margin)+","+Age;
		 ViableProductSubClassMap.put(margin, new Text(answer));
		 
		 if(ViableProductSubClassMap.size()>5)ViableProductSubClassMap.remove(ViableProductSubClassMap.firstKey()); 
		 
					 
		}	 
	 protected  void cleanup (Context context) throws IOException, InterruptedException  
		{
			for (Text t : ViableProductSubClassMap.descendingMap().values()) 
			{
			context.write(NullWritable.get(), t);
			}
			
		}
 }
 public static void main(String args[]) throws  IOException, ClassNotFoundException, InterruptedException
	{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Day Wise");
 job.setJarByClass(ViableProductSubClass.class);
 job.setMapperClass(ViableProductSubClassMapper.class);
 job.setPartitionerClass(ViableProductSubClassPartitioner.class);
 job.setReducerClass(ViableProductSubClassReducer.class);
 job.setNumReduceTasks(12);
 
 job.setMapOutputKeyClass(Text.class);
 job.setMapOutputValueClass(Text.class);
 
 job.setOutputKeyClass(NullWritable.class);
 job.setOutputValueClass(Text.class);
 
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

 

