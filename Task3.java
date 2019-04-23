import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task3 {
	
  public static class Task3Mapper
       extends Mapper<Object, Text, Text, DoubleWritable>{
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	String[] words = value.toString().split(",");

      Object foo = "";

      if(!(words[1] instanceof String)) {
        for (int i=0; i < words.length ; i++) {
          String word = words[i];
          Double count = Double.parseDouble(word);
          if (i==1) {
              context.write(new Text("MalesCount"), new DoubleWritable(count));
          }
          else if (i==2) {
              context.write(new Text("FemalesCount"), new DoubleWritable(count));
          }
          else if (i==3) {
            context.write(new Text("MalesLitracy"), new DoubleWritable(count));
          }
          else if (i==4) {
            context.write(new Text("FemalesLitracy"), new DoubleWritable(count));
          }			
        }	
      }
    }
  }

  public static class Task3Reducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

    	int count = 0;
    	Double sum = new Double(0);
    	for (DoubleWritable val : values) {
    		sum += val.get();
    		count++;
    	}

      value = sum;
      if(key.equals("MalesLitracy")){
          value = sum / count;
      }

      if(key.equals("FemalesLitracy")){
          value = sum / count;
      }
      context.write(new Text(key), new DoubleWritable(value));
    }
  }
  

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Stats");
    job.setJarByClass(Task3.class);
    job.setMapperClass(Task3Mapper.class);
    job.setCombinerClass(Task3Reducer.class);
    job.setReducerClass(Task3Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}