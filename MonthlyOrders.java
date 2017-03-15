//2
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MonthlyOrders {
	public static class MonthlyOrdersMapper extends
	Mapper<LongWritable, Text, Text,Text > 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] record = value.toString().split(";");
			context.write(new Text(toMonth(record[0])),new Text(record[8]));
		}
		String toMonth(String date) {
			   SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			   Calendar cal=Calendar.getInstance();
			   Date dateFrm = null;
			   
			   try {
			    dateFrm = format.parse(date);
			    
			    cal.setTime(dateFrm);
			    
			   } catch (ParseException e) {
			    e.printStackTrace();
			   }
			   String[] Month = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
			return Month[cal.get(Calendar.MONTH)];
		}
	}
	public static class MonthlyOrdersPartitioner extends
	   Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	    	  String[] str = key.toString().split(",");
		      if(str[0].trim().equals("January"))
		    	  return 0;
		      else if(str[0].trim().equals("February"))
		    	  return 1;
		      else if(str[0].trim().equals("March"))
		    	  return 2;
		      else if(str[0].trim().equals("April"))
		    	  return 3;
		      else if(str[0].trim().equals("May"))
		    	  return 4;
		      else if(str[0].trim().equals("June"))
		    	  return 5;
		      else if(str[0].trim().equals("July"))
		    	  return 6;
		      else if(str[0].trim().equals("August"))
		    	  return 7;
		      else if(str[0].trim().equals("September"))
		    	  return 8;
		      else if(str[0].trim().equals("October")) 
		    	  return 9;
		      else if(str[0].trim().equals("November"))
		    	  return 10;
		      else if(str[0].trim().equals("December"))
		    	  return 11;
		      
		      return 12;
		        
	      }
}
	public static class MonthlyOrdersReducer extends Reducer <Text, Text, NullWritable, Text> {
		 private TreeMap<LongWritable, Text> MonthlyOrders = new TreeMap<LongWritable, Text>();
		long SUMqty=0;String answer=null;int count=0;
		public void reduce(Text key, Iterable <Text> value, Context context) throws IOException, InterruptedException
		{		long orders=0;
			for(Text val:value)
			 {	orders++;
				String[] record = val.toString().split(",");
				SUMqty=SUMqty+Long.parseLong(record[0]);
				answer=key.toString()+","+Long.toString(SUMqty)+","+orders;
			 }
			MonthlyOrders.put(new LongWritable(SUMqty),new Text(answer));
			if (MonthlyOrders.size()>5)
				MonthlyOrders.remove(MonthlyOrders.firstKey());
		}
			protected  void cleanup (Context context) throws IOException, InterruptedException  
			{
				for (Text t : MonthlyOrders.descendingMap().values()) 
				context.write(NullWritable.get(), t);
			}
	}
	public static void main(String args[]) throws  IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Monthly Orders");
	    job.setJarByClass(MonthlyOrders.class);
	    job.setMapperClass(MonthlyOrdersMapper.class);
	    job.setPartitionerClass(MonthlyOrdersPartitioner.class);
	    job.setReducerClass(MonthlyOrdersReducer.class);
	    job.setNumReduceTasks(13);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
