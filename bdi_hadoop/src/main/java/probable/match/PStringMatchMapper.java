package probable.match;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PStringMatchMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	
	//Expect tab separated file as an input. Sample file is included in data path
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String file = value.toString();
		String[] lines = file.split("\n");
		int lineLength = lines.length;
		StringBuilder keyBuilder = new StringBuilder();
		StringBuilder valueBuilder = new StringBuilder();
		
		Text keyT = new Text();
		Text valueT = new Text();
		
		for (int i = 0; i < lineLength; i++) {
			int colLength = lines[i].length();
			String[] colValues = lines[i].toLowerCase().replace("\"", "").split("\t");

			if (colLength > 12) {
					keyBuilder.append(colValues[6]).append("\t").append(colValues[7])
					.append("\t").append(colValues[9]).append("\t").append(colValues[12]).append("\t").append(colValues[13]);
					
					valueBuilder.append(colValues[10]).append("\t").append(colValues[11]).append("\t").append(colValues[0]);
					
					keyT.set(keyBuilder.toString());
					valueT.set(valueBuilder.toString());
					context.write(keyT, valueT);
			}
		}
	}
}