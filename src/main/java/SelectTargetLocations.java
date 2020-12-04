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

    final static int COUNTRY_NAME = 0;
    final static int DATE = 4;
    final static int SCHOOL_CLOSING = 5;
    final static int SCHOOL_CLOSING_NOTES = 7;
    final static int WORKPLACE_CLOSING = 8;
    final static int WORKPLACE_CLOSING_NOTES = 10;
    final static int CANCEL_PUBLIC_EVENTS = 11;
    final static int CANCEL_PUBLIC_EVENTS_NOTES = 13;
    final static int RESTRICTIONS_ON_PUBLIC_GATHERINGS = 14;
    final static int RESTRICTIONS_ON_PUBLIC_GATHERINGS_NOTES =16;
    final static int CLOSE_PUBLIC_TRANSIT = 17;
    final static int CLOSE_PUBLIC_TRANSIT_NOTES = 19;
    final static int STAY_AT_HOME_REQ = 20;
    final static int STAY_AT_HOME_REQ_NOTES = 22;
    final static int INTERNAL_MOVEMENT_RESTRICTIONS = 23;
    final static int INTERNAL_MOVEMENT_RESTRICTIONS_NOTES = 25;


    public static class SelectTargetLocationsMapper extends Mapper<Object, Text, Text, NullWritable>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String CSVLine = value.toString();
            System.out.println(CSVLine);
            String countryName = CSVLine.split(",")[0];
            if (countryName.equals("United States")){
                Text countryValue = new Text(CSVLine);
                context.write(countryValue, NullWritable.get());
            }
        }
    }

    //Job 2: Narrow down US locations

    /**
     * This mapper class is used to narrow down us region such that the resulting aggregation contains regions that
     * have had frequent and relatively intense Covid lockdown protocols. It utilizes the final fields at the top of this
     * file, these fields are in accordance to the indices of their correlated data within the original dataset schema.
     */
    public static class NarrowUSStatesMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineEntry = value.toString().split(",");

            if (lineEntry.length >= 9 && !lineEntry[0].equals("")) {
                if (!lineEntry[SCHOOL_CLOSING].equals("") && Double.parseDouble(lineEntry[SCHOOL_CLOSING]) > 2.0) { //Filter only those entries that required school closing
                    String filteredEntryKey = extractStateAndDate(lineEntry);
                    String filteredEntryValue = extractValues(value.toString(), lineEntry);

                    Text mapperKey = new Text(filteredEntryKey);
                    Text mapperValue = new Text(filteredEntryValue);
                    context.write(mapperKey, mapperValue);
                }
            }
        }

        public String extractStateAndDate(String[] lineEntry){
            String cleanedEntry = "";
            cleanedEntry += lineEntry[2]; //extract State name
            cleanedEntry += lineEntry[5]; //extract DATE stamp
            return cleanedEntry;
        }

        public String extractValues(String lineEntry, String[] splitValues){
            return lineEntry.substring((splitValues[0].length() + splitValues[1].length() + splitValues[2].length() +
                    splitValues[3].length() + splitValues[4].length() + splitValues[5].length()));
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Select US locations only");
        job.setJarByClass(SelectTargetLocations.class);
        job.setMapperClass(SelectTargetLocationsMapper.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]  + "/SelectTargetLocations/selectUnitedStatesLocations"));
        job.waitForCompletion(true);

        //Job 2: Narrow down hotspot states with the restrictions they implement
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Narrow down selected targets");
        job2.setJarByClass(SelectTargetLocations.class);
        job2.setMapperClass(NarrowUSStatesMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
//        job2.setReducerClass(NarrowUSStates.class);
        job2.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job2, new Path(args[1]  + "/SelectTargetLocations/selectUnitedStatesLocations"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/SelectTargetLocations/narrowUnitedStatesLocations"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
