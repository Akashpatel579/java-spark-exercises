package df;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.to_date;

public class Q08SortNYSEDataByDateAndVolumeDesc {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession
				.builder()
				.master(args[0])
				.appName("Sort NYSE Data By Date and Volume")
				.getOrCreate();

		System.out.println(spark.conf().getAll());

		spark.conf().set("spark.sql.shuffle.partitions", 21);

		// Read NYSE Data
		// Input base directory should be passed as argument

		String nyseSchema = "stock_name STRING, date STRING, open FLOAT, close FLOAT, high FLOAT, low FLOAT, volume INT";
		Dataset<Row> df_nyse = readData(spark, args[1], "csv", nyseSchema);

//		date - year-month-day, 2018-08-08
//		volume - int, volume of the day
//		open - float, opening price of the day
//		close - float, closing price of the day
//		high - float, highest price of the day
//		low - float, lowest price of the day
//		adjclose - float, adjusted closing price of the day

		df_nyse.show(100);

		// Process using Data Frame Operations
		// Sort Data in ascending order by Date and descending order by Volume
		Dataset<Row> df_ordered_by_date_and_volume = processData(df_nyse);

		df_ordered_by_date_and_volume.show(400);

		// Write Output in CSV file format using gzip compression
		String outputFormat = "csv";
		writeOutputToLocal(df_ordered_by_date_and_volume, args[2], outputFormat );

//		Dataset<Row> readStoredData = readData(spark, "/Users/akashpatel/Documents/Clairvoyant/itversity/output/Q08SortNYSEDataByDateAndVolumeDesc/part-00011-9386682d-85bd-4925-8424-1ce15eeece36-c000.*", "csv", nyseSchema);
//		readStoredData.show();
	}

	private static void writeOutputToLocal(Dataset<Row> df, String outputPath, String outputFormat) {

		df.write().mode(SaveMode.Overwrite).option("header", true).format(outputFormat).save(outputPath);

	}

	private static Dataset<Row> processData(Dataset<Row> df) {
		Dataset<Row> processed_data = df
								.withColumn("date", to_date(df.col("date"), "yyyyMMdd"))
								.select( "*")
								.orderBy(df.col("date"), df.col("Volume").desc());

		return  processed_data;
	}

	private static Dataset<Row> readData(SparkSession spark, String datasetPath, String datasetFormat, String datasetSchema) {

		Dataset<Row> df= spark.read().format(datasetFormat).schema(datasetSchema).load(datasetPath);

		return df;
	}

}
