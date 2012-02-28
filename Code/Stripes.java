import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class Stripes {
/**
 * The map class of WordCount.
 */
public static class TokenCounterMapper
    extends Mapper<Object, Text, Text, Text > {
        
 

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
    	
    	String s1,s2;
    	Integer no;
    	Map<String,Map<String,Integer>> outer = new HashMap<String,Map<String, Integer>>();
    	Map<String, Integer> in_temp = new HashMap<String, Integer>();
		StringTokenizer itr = new StringTokenizer(value.toString());
		StringTokenizer itr2 = new StringTokenizer(value.toString());
		int i = 0;
		int j = 0;
		while (itr2.hasMoreTokens())
		{	

        	s1=itr2.nextToken();
        	i++;
			itr = new StringTokenizer(value.toString());
			j=0;
        	while(itr.hasMoreTokens())
        	{	
        		 	s2= itr.nextToken();
        		 	j++;
	        	  	in_temp=outer.get(s1);
	        	  	
	        	  	if(i!=j)
	        	  	{
	        	  	if(in_temp!=null)
	        	  	{
        		    	no = in_temp.containsKey(s2) ? in_temp.get(s2) : 0;
  	        	    	in_temp.put(s2, no + 1);
  	        	    	outer.put(s1,in_temp);
	        	  	}
        	
	        	  	else
	        	  	{
	        	  		Map<String, Integer> inner = new HashMap<String, Integer>();
	        	  		inner.put(s2,1); 
	        	  		outer.put(s1,inner);
	        	  	}
	        	  	}
	        	  	//s1=s2;
    		}        	
        }
        
        for (Entry<String,Map<String, Integer>> e : outer.entrySet())
  	    {
        	 s1=e.getKey();
  		     String val="";
    	    	
  		  for (Entry<String, Integer> e1 : e.getValue().entrySet())
  		  {
  			  val+=e1.getKey()+" "+e1.getValue()+" ";
  		  }
  		 Text output_key= new Text();
  		 output_key.set(s1);
  		Text output_value= new Text();
 		 output_value.set(val);
 		 
  		  context.write(output_key,output_value);
  	   }
  	  
    }
 }

        
/**
 * The reducer class of WordCount
 */

public static class TokenCounterReducer
    extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key,Iterable<Text> values , Context context)
     
        throws IOException, InterruptedException {
        String op="";
        Integer total=0;
        Map<String, Integer> inner = new HashMap<String, Integer>();
    	for (Text value : values) 
    	  {
    		StringTokenizer itr = new StringTokenizer(value.toString());
            String word;
            Integer count,no;
    	        	while(itr.hasMoreTokens())
    	        	{	
    	        		word=itr.nextToken();
    	        		if(word.compareTo("")!=0)
    	        		{
    	        		count= Integer.parseInt(itr.nextToken());
    		        	no = inner.containsKey(word) ? inner.get(word) : 0;
    	  	        	inner.put(word,no+count);
    	  	        	total+=count;
    	        		}
    	        		else 
    	        			 break;
    	    		}        	
         }

    	for (Entry<String, Integer> e1 : inner.entrySet())
  		  {
  			  op+=e1.getKey()+" "+(float)e1.getValue()/(float)total+" ";
  		
  		  }
    	Text output_value= new Text();
		 output_value.set(op);
		
    	context.write(key,output_value);
    	
    }
}

public static class WordPartitioner extends Partitioner<Text, Text> {
	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		String a=key.toString();
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
    Job job = new Job(conf, "Stripes");
    job.setJarByClass(Stripes.class);
    job.setMapperClass(TokenCounterMapper.class);
   job.setReducerClass(TokenCounterReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setPartitionerClass(WordPartitioner.class);
    job.setNumReduceTasks(2);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
   }


}
