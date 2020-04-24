package df;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.sum;

public class Q06ComputeDailyRevenue {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession
				.builder()
				.master(args[0])
				.appName("Read and Write APIs")
				.getOrCreate();
		
		// Read Orders and Order Items Data
		// Input base directory should be passed as argument
		String ordersSchema = "order_id INT, order_date STRING, order_customer_id INT, order_status STRING";

		String ordersItemsSchema = "order_item_id INT, order_item_order_id INT, order_item_product_id INT, " +
				"order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT";

		Dataset<Row> df_orders = readData(spark, args[1], "csv", ordersSchema);

		Dataset<Row> df_order_items = readData(spark, args[2], "csv", ordersItemsSchema);

		df_orders.show();

		df_order_items.show();



		// Process using Data Frame Operations
		// Filter for all complete or closed orders
		// Aggregate using order_date as key

		Dataset<Row> df_filtered_orders = completeAndClosedOrdersByDate(df_orders);

//		df_filtered_orders.show();

		Dataset<Row> agg_total_revenue_by_date = totalRevenueByDate(df_filtered_orders, df_order_items);

		// Write Output in avro file format with out compression
		// Output directory should be passed as argument
		// Output should contain order_date and revenue
		// Data should be sorted by order_date
		String outputFormat = "parquet";
		writeOutputToLocal(spark, agg_total_revenue_by_date, args[3], outputFormat);

	}

	private static void writeOutputToLocal(SparkSession spark, Dataset<Row> df, String outputPath, String outputFormat) {

		df.coalesce(2).write().mode(SaveMode.Overwrite).format(outputFormat).save(outputPath);

	}
	private static Dataset<Row> totalRevenueByDate(Dataset<Row> df_filtered_orders, Dataset<Row> df_order_items) {

		df_order_items.printSchema();

		Dataset<Row> total_revenue  = df_order_items.select("order_item_order_id", "order_item_subtotal")
				.groupBy(df_order_items.col("order_item_order_id"))
				.agg(sum(df_order_items.col("order_item_subtotal")).as("total_revenue"));

		return df_filtered_orders
				.join(total_revenue, total_revenue.col("order_item_order_id")
										.equalTo(df_filtered_orders.col("order_id")))
				.select(df_filtered_orders.col("order_date"),
						total_revenue.col("total_revenue"))
				.groupBy(df_filtered_orders.col("order_date"))
				.agg(sum("total_revenue").as("total_revenue"))
				.orderBy(df_filtered_orders.col("order_date"));

	}

	private static Dataset<Row> completeAndClosedOrdersByDate(Dataset<Row> df){

		String[] orderStatuses = new String[2];
		orderStatuses[0] = "COMPLETE";
		orderStatuses[1] = "CLOSED";

		Dataset<Row> dff = df.filter(df.col("order_status").isin(orderStatuses));
		return dff;
	}

	private static Dataset<Row> readData(SparkSession spark, String datasetPath, String datasetFormat, String tblSchema) {

		Dataset<Row> df= spark.read().format(datasetFormat).schema(tblSchema).load(datasetPath);

		return df;
	}


}
