import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Comparator;
import java.util.LinkedHashMap;


public class Task2 {
	
  public static class Task2Mapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class Task2Reducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
  
    private Map<String, Integer> wordsHashMap = new HashMap<>();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      wordsHashMap.put(key.toString(), sum);

      wordsHashMap = Task2Reducer.sortByValue(wordsHashMap);

      if (wordsHashMap.size() > 20) {
            Map<String, Integer> players = wordsHashMap;
            List<String> list = new ArrayList<String>(players.keySet());
            wordsHashMap.remove(list.get(list.size()-1));
      }
    }
    
 	public static HashMap<String, Integer> sortByValue(Map<String, Integer> hash) 
 	{ 
 		List<Map.Entry<String, Integer> > li = new LinkedList<Map.Entry<String, Integer> >(hash.entrySet()); 
 		Comparator<Map.Entry<String, Integer> > com =  new Comparator<Map.Entry<String, Integer> >() { 
  			
  			public int compare(Map.Entry<String, Integer> firstItem, Map.Entry<String, Integer> secoundItem) 
  			{ 
  				return  -1 * (firstItem.getValue()).compareTo(secoundItem.getValue()); 
  			} 
  		};

 		Collections.sort(li, com); 
 		
 		HashMap<String, Integer> res = new LinkedHashMap<String, Integer>(); 
 		
 		for (Map.Entry<String, Integer> item : li) { 
 			res.put(item.getKey(), item.getValue()); 
 		}
 		
 		return res; 
 	} 
    
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	
        for (String key : wordsHashMap.keySet()) {
            context.write(new Text(key), new IntWritable(wordsHashMap.get(key)));
        }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "N Top Words");
    job.setJarByClass(Task2.class);
    job.setMapperClass(Task2Mapper.class);
    job.setCombinerClass(Task2Reducer.class);
    job.setReducerClass(Task2Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}