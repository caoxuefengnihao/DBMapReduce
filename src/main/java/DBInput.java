import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DBInput extends Mapper<LongWritable,WordValue,Text, Text> {


    @Override
    protected void map(LongWritable key, WordValue value, Context context) throws IOException, InterruptedException {
        context.write(new Text(value.word),new Text(value.value) );
    }
}
