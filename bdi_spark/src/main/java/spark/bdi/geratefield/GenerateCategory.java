package spark.bdi.geratefield;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class GenerateCategory {
	private static String appName = "URLTrafficCal";
	private static String master = "local";

	public static void main(String[] args) throws IOException {
		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		try (JavaSparkContext context = new JavaSparkContext(conf)) {
			SparkSession session = SparkSession.builder().appName(appName).master(master)
					.config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse").getOrCreate();

			Properties prop = new Properties();
			String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
			String propFileName = rootPath + "category.properties";

			InputStream inputStream = GenerateCategory.class.getClassLoader().getResourceAsStream(propFileName);

			if (inputStream != null) {
				prop.load(new FileInputStream(propFileName));
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}

			JavaRDD<String> dataFile = context.textFile("data/products.csv");
			String header = dataFile.first();
			JavaRDD<Tuple2<String, String>> tuple = dataFile.filter(x -> !x.equals(header))
					.map(x -> x.replace("\"", "")).map(x -> x.toLowerCase())
					.map(x -> new Tuple2<String, String>(x.split(",")[0], x.split(",")[2]));

			tuple.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
				public Tuple2<String, String> call(Tuple2<String, String> item) {

					String[] keyWords = item._2.split(" ");
					HashMap<String, Integer> hm = new HashMap();

					for (int i = 0; i < keyWords.length; i++) {
						String key = prop.getProperty(keyWords[i]);
						System.out.println(keyWords[i] + " key " + key);
						if (hm.containsKey(key))
							hm.put(key, hm.get(key) + 1);
						else
							hm.put(key, 1);
					}

					int maxValueInMap = (Collections.max(hm.values()));
					int maxCount = 0;
					String interestedCategory = "";
					for (Entry<String, Integer> entry : hm.entrySet()) {
						if (entry.getValue() == maxValueInMap) {
							maxCount++;
							interestedCategory = entry.getKey();
						}
					}
					return new Tuple2<String, String>(item._1, interestedCategory);
				}
			}).saveAsTextFile("out//out.csv");
		}
	}
}