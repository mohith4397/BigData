//3a Age Wise
import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopGrossProdID {

	public static class TopGrossProdIDMapper extends
	Mapper<LongWritable, Text, Text,Text > {
		long sales=0,cost=0,profit=0;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String record = value.toString();
			String[] parts = record.split(";");
			 sales = Long.parseLong(parts[8]);
			 cost = Long.parseLong(parts[7]);
			 profit=sales-cost;
			Text finalkey=new Text(parts[5]); //prodid
			String finalvalue=parts[2]+","+String.valueOf(profit);
			context.write(finalkey, new Text(finalvalue));
		}
		
		
	}
	
		public static class TopGrossProdIDPartitioner extends
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
			      else
			    	  return 11;
			    	  
		      }
		      
		   }
	public static class TopGrossProdIDReducer extends Reducer <Text, Text, NullWritable, Text> {
		 private TreeMap<LongWritable, Text> GrossMap = new TreeMap<LongWritable, Text>();
		long sumprofit=0;String answer=null;int count=0;
		public void reduce(Text key, Iterable <Text> value, Context context) throws IOException, InterruptedException
		{
			for(Text val:value)
			 {
				String[] str = val.toString().split(",");
				sumprofit=sumprofit+Long.parseLong(str[1]);
				answer=key.toString()+","+Long.toString(sumprofit)+","+str[0];
			 }
			GrossMap.put(new LongWritable(sumprofit),new Text(answer));
			if (GrossMap.size()>5)
				GrossMap.remove(GrossMap.firstKey());
		}
			protected  void cleanup (Context context) throws IOException, InterruptedException  
			{
				for (Text t : GrossMap.values()) 
				context.write(NullWritable.get(), t);
			}
	}
	
	public static void main(String args[]) throws  IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Gross Sales");
	    job.setJarByClass(TopGrossProdID.class);
	    job.setMapperClass(TopGrossProdIDMapper.class);
	    job.setPartitionerClass(TopGrossProdIDPartitioner.class);
	    job.setReducerClass(TopGrossProdIDReducer.class);
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
