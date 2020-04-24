package df;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Q02GetAllCompleteOrClosedOrdersForAGivenDay {

//	command line arguments

//	local
//	/Users/akashpatel/Documents/Clairvoyant/itversity/dataset/orders
//	"2013-07-25 00:00:00.0"
//	/Users/akashpatel/Documents/Clairvoyant/itversity/output/Q02GetAllCompleteOrClosedOrdersForAGivenDay

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession
				.builder()
				.master(args[0])
				.appName("Get all Complete and Closed Orders for a given day")
				.getOrCreate();
		
		// Read Orders Data
		// Input base directory should be passed as argument
		Dataset<Row> df = spark.read()
				.schema("order_id INT, order_date STRING, order_customer_id INT, order_status STRING")
				.csv(args[1]);

		df.show();

		// Process using Data Frame Operations
		// Filter for all complete or closed orders for a given day passed as argument
		// Dataset<Row> dff = df.filter("order_status IN ('COMPLETE', 'CLOSED')");
		Dataset<Row> dff = 	completeAndClosedOrdersByDate(df, args[2]);
		dff.show();

		// Write Output in pipe delimited format into text files
		// Output directory should be passed as argument
		// Output should contain all the fields from orders data set
		writeOutputToLocal(dff, args[3]);



		spark.stop();

	}

	private static void writeOutputToLocal(Dataset<Row> dff, String outputPath) {

		dff.write().mode(SaveMode.Overwrite).format("csv").option("header", true).save(outputPath);
	}

	public static Dataset<Row> completeAndClosedOrdersByDate(Dataset<Row> df, String date){

		String[] orderStatuses = new String[2];
		orderStatuses[0] = "COMPLETE";
		orderStatuses[1] = "CLOSED";


		System.out.println("date >> " + date);
		Dataset<Row> dff = df.filter(df.col("order_status").isin(orderStatuses)
				.and(df.col("order_date").equalTo(date)));
		return dff;
	}


}
