import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Sentiment
{
	static Map<String, Integer> SentimentMap = new HashMap<String, Integer>();
	public static class SentimentMap extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
		
		protected void setup(Context context) throws  IOException, InterruptedException 
		{
			super.setup(context);
			URI[]  files=context.getCacheFiles();
			Path p = new Path(files[0]);
			if(p.getName().equals("AFINN.txt"))
			{
				BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
				String line = reader.readLine();
				while(line != null) {
				String[] tokens = line.split("\t");
				SentimentMap.put(tokens[0], Integer.parseInt(tokens[1]));
				line = reader.readLine();
						}
				reader.close();
		}
		}
	      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	      {  	  
	    	  StringTokenizer itr=new StringTokenizer(value.toString());
		         while (itr.hasMoreTokens())
		         { 
		        	 String a=itr.nextToken();
		        	 if(SentimentMap.containsKey(a))
		        	 {
		        		 if (SentimentMap.get(a)>=0)
		        			 context.write(new Text("positive"),new LongWritable(SentimentMap.get(a)));
		        		 else
		        			 context.write(new Text("negative"),new LongWritable(SentimentMap.get(a)));
		        			 
		        	 }
		        		 
		         }
	    	  
	    
	      }
	   }
	
	public static class SentimentReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{  int pos=0,neg=0;
		private LongWritable result=new LongWritable(); int[] arr;
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum=0;
			for(LongWritable val:values)
			sum+=Math.abs(val.get());
			result.set(sum); 
			if(key.toString().equals("positive"))
				pos=sum;
			if(key.toString().equals("negative"))
				neg=sum;	
//			context.write(new Text(key), result);
		}
		protected  void cleanup (Context context) throws IOException, InterruptedException  
		{
			double answer=100%( (double)((pos-neg)*100) /(double)(pos+neg) );
			context.write(new Text("Sentiment Percent "+answer+"%."), null);
		}	
	}

public static void main(String args[]) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf);
    job.setJarByClass(Sentiment.class);
    job.setJobName("Sentiment Analysis");
    job.addCacheFile(new Path("AFINN.txt").toUri());
    
    job.setMapperClass(SentimentMap.class);
	job.setReducerClass(SentimentReducer.class);
//	job.setNumReduceTasks(0);
	
	job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(LongWritable.class);
		
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

	
	
}