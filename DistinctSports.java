import java.io.IOException;
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

public class DistinctSports {

	public static class DistinctSportsMapper extends Mapper<LongWritable,Text, Text, NullWritable> 
	{
		protected void map(LongWritable key, Text value, Context context)
	            throws java.io.IOException, InterruptedException 
		{
			
        	context.write(value, NullWritable.get());
        	
			
		}
	}		

	public static class DistinctSportsPartitioner extends
	   Partitioner < Text, NullWritable >
	   {
	      @Override
	      public int getPartition(Text key, NullWritable value, int numReduceTasks)
	      {
	         String[] str = key.toString().split(",");
	      if(str[4].equals("Air Sports"))
	            return 0;
	      if(str[4].equals("Combat Sports")) 
	            return 1 ;
	      if(str[4].equals("Dancing"))
	    	  return 2 ;
	      if(str[4].equals("Exercise & Fitness"))
	    	  return 3 ;
	     if(str[4].equals("Games"))
	    	 return 4 ;
	    if(str[4].equals("Gymnastics"))
	    	return 5 ;
	    if(str[4].equals("Indoor Games"))
	    	return 6 ;
	    if(str[4].equals("Jumping"))
	    	return 7 ;
	    if(str[4].equals("Outdoor Play Equipment"))
	    	return 8 ;
	    if(str[4].equals("Outdoor Recreation"))
	    	return 9 ;
	    if(str[4].equals("Puzzles"))
	    	return 10 ;
	    if(str[4].equals("Racquet Sports"))
	    	return 11 ;
	    if(str[4].equals("Team Sports"))
	    	return 12 ;
	    if(str[4].equals("Water Sports"))
	    	return 13 ;
	    if(str[4].equals("Winter Sports"))
	    	return 14 ;
	    else
	    	return 15;
  
	      
	      }
	   }
		public static class  DistinctSportsReducer extends Reducer  <Text,NullWritable, Text, NullWritable> 
		{
		
			protected void reduce (Text key,  Iterable <NullWritable> values, Context context)
		            throws java.io.IOException, InterruptedException
		            {
				context.write(new Text(key), NullWritable.get());
		            }
		}
	
	
	public static void main(String args[]) throws  IOException, ClassNotFoundException, InterruptedException 
	{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Distinct Sports");
	    job.setJarByClass(DistinctSports.class);
	    job.setMapperClass(DistinctSportsMapper.class);
	    job.setPartitionerClass(DistinctSportsPartitioner.class);
	    job.setReducerClass(DistinctSportsReducer.class);
	    job.setNumReduceTasks(16);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	}
