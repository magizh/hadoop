import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Pairs {
/**
 * The map class of WordCount.
 */
public static class TokenCounterMapper
    extends Mapper<Object, Text, TextPair, IntWritable> {
        
    private final static IntWritable one = new IntWritable(1);
   // private Text word = new Text();

	public void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
		String a;
		String b;
		TextPair p ;
		StringTokenizer itr = new StringTokenizer(value.toString());
		StringTokenizer itr2 = new StringTokenizer(value.toString());
		int i = 0;
		int j = 0;
		while (itr2.hasMoreTokens())
		{	
			a= itr2.nextToken();
			i++;
			j=0;
			itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {

				if(itr.hasMoreTokens())
				{

					b= itr.nextToken();
					j++;
				}
				else
				{
					b = "";

				}
				if (i!=j)
				{
					p = new TextPair(a,b);
					context.write(p, one);
					p = new TextPair(a,"*");
					context.write(p,one);
				}
			}
		}
	}
}

/**
 * The reducer class of WordCount
 */
public static class TokenCounterReducer
    extends Reducer<TextPair, IntWritable, TextPair, Float> {
	Integer star_count=1;
	String s1;
    public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        s1=key.getSecond().toString();
        if(s1.compareTo("*")==0)
            {
        	 star_count=sum;
            //context.write(key, new IntWritable(sum));
            }
        else
        	context.write(key,(float)sum/(float)star_count);
    }
}

public static class WordPartitioner extends Partitioner<TextPair, IntWritable> {
	@Override
	public int getPartition(TextPair key, IntWritable value, int numPartitions) {
		String a=key.getFirst().toString();
		char c[]=a.toCharArray();
	  if((c[0]>='a'&&c[0]<='m')||(c[0]>='A'&&c[0]<='M'))
		  return 0;
	  else
	return 1;
	}
	
}

	/**
 * The main entry point.
 */
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = new Job(conf, "Pairs");
    job.setJarByClass(TextPair.class);
    job.setMapperClass(TokenCounterMapper.class);
    job.setReducerClass(TokenCounterReducer.class);
     
    job.setOutputKeyClass(TextPair.class);
    job.setOutputValueClass(Float.class);
    job.setMapOutputKeyClass(TextPair.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(2);
    job.setPartitionerClass(WordPartitioner.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}




}