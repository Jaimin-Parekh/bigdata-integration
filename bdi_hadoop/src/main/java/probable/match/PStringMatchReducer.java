package probable.match;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PStringMatchReducer extends Reducer<Text, Text, Text, Text> {
	double matchCrieterion = 0.5;
	// needs to be less than 1, no extra validation implemented for value check

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		List<String> names = new ArrayList<String>();
		List<String> categories = new ArrayList<String>();
		HashMap<String, String> nameId = new HashMap<String, String>();
		boolean writeFlag;
		for (Text val : values) {
			String[] strs = val.toString().split("\t");
			names.add(strs[0]);
			categories.add(strs[1]);
			nameId.put(strs[0] + "\t" + strs[1], strs[2]);
		}

		int nameSize = names.size();
		outer: for (int i = 0; i < nameSize; i++) {
			writeFlag = true;
			inner: for (int j = i + 1; j < nameSize; j++) {
				if (probableMatch(names.get(i), names.get(j))
						&& probableMatch(categories.get(i), categories.get(j))) {
					writeFlag = true;
					break inner;
				}
			}
			if (writeFlag) {
				// equal number of value for name and categories
				String hashKey = names.get(i) + "\t" + categories.get(i);
				context.write(new Text(nameId.get(hashKey)), new Text(hashKey));
			}
		}
	}

	public boolean probableMatch(String sourceString, String destString) {
		List<String> source = Arrays.asList(sourceString.split(" "));
		List<String> dest = Arrays.asList(destString.split(" "));
		int counter1 = 0;
		int counter2 = 0;

		for (String str : source) {
			if (dest.contains(str))
				counter1++;
		}
		for (String str : dest) {
			if (source.contains(str))
				counter2++;
		}
		if (counter1 > matchCrieterion * source.size()
				|| counter2 > matchCrieterion * source.size())
			return true;

		return false;
	}
}