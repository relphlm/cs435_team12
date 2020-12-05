import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.javatuples.Pair;


import java.io.IOException;
import java.util.ArrayList;

public class EntriesDescending {

   public static class StatesDescendingMapper extends Mapper<Object, Text, Text, IntWritable>{

       @Override
       protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           String line = value.toString();
           String[] lineSplitValues = line.split(",");
           Text mapperKey = new Text(lineSplitValues[0].split(":")[0]);
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
            String[] lineSplit = value.toString().split(" ");
            Text mapperKey = new Text(lineSplit[0]);
            IntWritable mapperVal = new IntWritable(Integer.parseInt(lineSplit[1]));
            context.write(mapperKey, mapperVal);
        }
    }

    public static class DescendingCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private ArrayList<Pair<String, Integer>> final100EdgeOut;

        @Override
        protected void setup(Context context) {
            final100EdgeOut = new ArrayList<>();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
            for (IntWritable edge : values) {
//                System.out.println("Reducer PUT: " + key.toString() + ", " +  edge.get());
                Pair tempPair = new Pair(key.toString(), edge.get());
                final100EdgeOut.add(tempPair);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            final100EdgeOut.sort((o1, o2) ->
                    o2.getValue1() - o1.getValue1());

            for (int i = 0; i < 100; i++) {
                Text state = new Text(final100EdgeOut.get(i).getValue0());
                IntWritable count = new IntWritable(final100EdgeOut.get(i).getValue1());
                context.write(state, count);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Count state entries");
        job1.setJarByClass(EntriesDescending.class);
        job1.setMapperClass(StatesDescendingMapper.class);
        job1.setReducerClass(StatesDescendingReducer.class);
        job1.setNumReduceTasks(5);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class
        );

        job1.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job1, new Path(args[0]  + "/SelectTargetLocations/3School3Work"));
        FileOutputFormat.setOutputPath(job1, new Path(args[0] + "/SelectTargetLocations/countTemp"));
        job1.waitForCompletion(true);

        //// job 2 ////////////////////////
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Order State Count in descending");
        job2.setJarByClass(EntriesDescending.class);
        job2.setMapperClass(DescendingCountMapper.class);
        job2.setReducerClass(DescendingCountReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job2, new Path(args[0] + "/SelectTargetLocations/countTemp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[0] + "/SelectTargetLocations/count"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);


    }

}
