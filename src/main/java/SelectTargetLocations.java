import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * The purpose of this class is to parse through the Covid_government_response dataset, and filter through states that have had a higher number of
 * imposed restrictions. This will allow us to narrow our focused locations within our ar quality datasets
 */

public class SelectTargetLocations {

    public static class SelectTargetLocationsMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String CSVLine = value.toString();
            System.out.println(CSVLine);
            String countryName = CSVLine.split(",")[0];
            if (countryName.equals("United States")){
                Text countryKey = new Text(countryName);
                Text countryValue = new Text(CSVLine.substring(countryName.length()));
                context.write(countryKey, countryValue);
            }
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Select target locations");
        job.setJarByClass(SelectTargetLocations.class);
        job.setMapperClass(SelectTargetLocationsMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]  + "/SelectTargetLocations/selectUnitedStatesLocations"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
