import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DayWise {
	public static class DayWiseMapper extends
	Mapper<LongWritable, Text, Text,Text > 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] record = value.toString().split(";");
			context.write(new Text(toDay(record[0])), new Text(record[8]));
		}
		String toDay(String date) {
			   SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			   Calendar cal=Calendar.getInstance();
			   Date dateFrm = null;
			   
			   try {
			    dateFrm = format.parse(date);
			    
			    cal.setTime(dateFrm);
			    
			   } catch (ParseException e) {
			    e.printStackTrace();
			   }
			   String[] days ={"Sunday","Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
			return days [cal.get(Calendar.DAY_OF_WEEK) -1];
		}
	}
	public static class DayWiseReducer extends
	Reducer<Text, Text, NullWritable,Text >
	{
		private TreeMap <DoubleWritable, Text> DayMap = new TreeMap<DoubleWritable, Text>();
		double GrandtotalSales=0;
		public void reduce(Text key, Iterable <Text> value, Context context) throws IOException, InterruptedException
		{		double SUMsales=0;
			for (Text val:value)
			{	String[] record=val.toString().split(",");
				SUMsales=SUMsales+Double.parseDouble(record[0]);
			}
			String answer=key.toString()+","+String.format("%.2f", SUMsales);
			GrandtotalSales=GrandtotalSales+SUMsales;
			DayMap.put(new DoubleWritable(SUMsales),new Text(answer));
			
		}
			protected  void cleanup (Context context) throws IOException, InterruptedException  
			{
				for (Text t : DayMap.descendingMap().values()) 
				{
					String[]  temp=t.toString().split(",");
					String answer=temp[0]+","+temp[1]+","+String.format("%.2f",(Double.parseDouble(temp[1])*100)/GrandtotalSales);
				context.write(NullWritable.get(), new Text(answer));
				}
				
			}
			
			
		
	}
	public static void main(String args[]) throws  IOException, ClassNotFoundException, InterruptedException
	{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Day Wise");
    job.setJarByClass(DayWise.class);
    job.setMapperClass(DayWiseMapper.class);
    job.setReducerClass(DayWiseReducer.class);
    
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
