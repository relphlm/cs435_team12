import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.ArrayList;

public class Top10StateEntries {

   public static class StatesDescendingMapper extends Mapper<Object, Text, Text, IntWritable>{

       @Override
       protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           String line = value.toString();
           String[] lineSplitValues = line.split(",");
           Text mapperKey = new Text(lineSplitValues[0].split(":")[0] + ":");
           IntWritable dummyVal = new IntWritable(1);
           context.write(mapperKey, dummyVal);
       }
   }

   public static class StatesDescendingReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

       @Override
       protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           int count = 0;
           for (IntWritable val :
                   values) {
               count++;
           }
           IntWritable finalCount = new IntWritable(count);
           context.write(key, finalCount);
       }

   }

    /////Job 2
    public static class DescendingCountMapper extends Mapper<Object, Text, Text, IntWritable>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineSplit = value.toString().split(":");
            Text mapperKey = new Text(lineSplit[0]);
            IntWritable mapperVal = new IntWritable(Integer.parseInt(lineSplit[1].trim()));
            context.write(mapperKey, mapperVal);
        }
    }

    public static class DescendingCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private ArrayList<Pair<String, Integer>> stateEntries;

        @Override
        protected void setup(Context context) {
            stateEntries = new ArrayList<>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
            for (IntWritable edge : values) {
//                System.out.println("Reducer PUT: " + key.toString() + ", " +  edge.get());
                Pair tempPair = new Pair(key.toString(), edge.get());
                stateEntries.add(tempPair);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            stateEntries.sort((o1, o2) ->
                    o2.getValue() - o1.getValue());

            for (int i = 0; i < 10; i++) {
                Text state = new Text(stateEntries.get(i).getKey());
                IntWritable count = new IntWritable(stateEntries.get(i).getValue());
                context.write(state, count);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Count state entries");
        job1.setJarByClass(Top10StateEntries.class);
        job1.setMapperClass(StatesDescendingMapper.class);
        job1.setReducerClass(StatesDescendingReducer.class);
        job1.setNumReduceTasks(1);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class
        );

        FileInputFormat.addInputPath(job1, new Path(args[0]  + "/SelectTargetLocations/selectControlStates"));
        FileOutputFormat.setOutputPath(job1, new Path(args[0] + "/SelectTargetLocations/TEMPTop10StateEntries"));
        job1.waitForCompletion(true);

        //// job 2 ////////////////////////
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Order State Count in descending");
        job2.setJarByClass(Top10StateEntries.class);
        job2.setMapperClass(DescendingCountMapper.class);
        job2.setReducerClass(DescendingCountReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job2, new Path(args[0] + "/SelectTargetLocations/TEMPTop10StateEntries"));
        FileOutputFormat.setOutputPath(job2, new Path(args[0] + "/SelectTargetLocations/Top10StateEntries"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);


    }

}
