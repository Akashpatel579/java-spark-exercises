package df;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;

public class Q09GetTopNStocksByVolume {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession
				.builder()
				.master(args[0])
				.appName("Read and Write APIs")
				.getOrCreate();
		
		// Read NYSE Data
		// Input base directory should be passed as argument
		System.out.println(spark.conf().getAll());

		spark.conf().set("spark.sql.shuffle.partitions", 21);

		// Read NYSE Data
		// Input base directory should be passed as argument

		String nyseSchema = "stock_name STRING, date STRING, open FLOAT, close FLOAT, high FLOAT, low FLOAT, volume INT";
		Dataset<Row> df_nyse = readData(spark, args[1], "csv", nyseSchema);
		// Process using Data Frame Operations
		// Get top 5 stocks by volume
		Dataset<Row> df_top_5_stocks_by_volume = processData(df_nyse);

		df_top_5_stocks_by_volume.show(400);

		
		// Write Output in CSV file format with out compression
		String outputFormat = "csv";
		writeOutputToLocal(df_top_5_stocks_by_volume, args[2], outputFormat );
		
	}

	private static void writeOutputToLocal(Dataset<Row> df, String outputPath, String outputFormat) {

		df.coalesce(1).write().mode(SaveMode.Overwrite).option("header", true).format(outputFormat).save(outputPath);

	}

	private static Dataset<Row> processData(Dataset<Row> df) {

		Dataset<Row> processed_data = df
				.select( "*")
				.groupBy(df.col("stock_name"))
				.agg(sum(df.col("volume")).as("total_volume"))
				.orderBy(sum(df.col("volume")).as("total_volume").desc())
				.limit(5);

		return  processed_data;
	}

	private static Dataset<Row> readData(SparkSession spark, String datasetPath, String datasetFormat, String datasetSchema) {

		Dataset<Row> df= spark.read().format(datasetFormat).schema(datasetSchema).load(datasetPath);

		return df;
	}

}
