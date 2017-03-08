import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Employee {

public static class EmployeeMapper extends Mapper<LongWritable,Text,Text,Text>
{
public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException
{
String str[]=values.toString().split(",");
String  gender=str[3];
String OutputString=str[1]+","+str[2]+","+str[4];//name+age+salary
context.write(new Text(gender), new Text(OutputString));
}
	
}
public static class EmployeeReducer extends Reducer<Text,Text,Text,IntWritable>	
{
	public void reduce(Text key,Iterable <Text> values,Context context) throws InterruptedException, IOException
	
	{ 
		int maxsalary=-1;
		for(Text val : values)
		{
		String str[]=val.toString().split(",");
		if(maxsalary>Integer.parseInt(str[2]))
		maxsalary=Integer.parseInt(str[2]);
		context.write(key,new IntWritable(maxsalary));
		}
		
		
	}
	
}
public static class EmployeePartitioner extends
Partitioner < Text, Text >
{
   @Override
   public int getPartition(Text key, Text value, int numReduceTasks)
   {
      String[] str = value.toString().split(",");
      int age = Integer.parseInt(str[2]);


      if(age<=20)
      {
         return 0;
      }
      else if(age>20 && age<=30)
      {
         return 1 ;
      }
      else
      {
         return 2;
      }
   }
}
public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException
{
	Configuration conf = new Configuration();
    //conf.set("name", "value")
    Job job = Job.getInstance(conf, "Employee");
    job.setJarByClass(Employee.class);
    job.setMapperClass(EmployeeMapper.class);
    job.setPartitionerClass(EmployeePartitioner.class);
    //job.setCombinerClass(ReduceClass.class);
    job.setReducerClass(EmployeeReducer.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
