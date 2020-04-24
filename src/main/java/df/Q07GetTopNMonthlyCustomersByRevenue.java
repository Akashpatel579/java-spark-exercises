package df;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class Q07GetTopNMonthlyCustomersByRevenue {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession
				.builder()
				.master(args[0])
				.appName("Read and Write APIs")
				.getOrCreate();
		
		// Read Orders, Order Items and Customers Data
		// Input base directory should be passed as argument
		String ordersSchema = "order_id INT, order_date STRING, order_customer_id INT, order_status STRING";

		String ordersItemsSchema = "order_item_id INT, order_item_order_id INT, order_item_product_id INT, " +
				"order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT";

		String customerSchema = "customer_id INT,first_name STRING,last_name STRING,contact_no1 STRING,contact_no2 STRING," +
				"address_line_1 STRING, city STRING, state STRING,pincode STRING";

		Dataset<Row> df_orders = readData(spark, args[1], "csv", ordersSchema);

		Dataset<Row> df_order_items = readData(spark, args[2], "csv", ordersItemsSchema);

		Dataset<Row> df_customers = readData(spark, args[3], "csv", customerSchema);

		df_orders.show();

		df_order_items.show();

		df_customers.show();
		// Process using Data Frame Operations
		// Filter for all complete or closed orders
		// Aggregate using order month derived from order_date and customer_id as key
		// Get ranking and filter for top N customers

		Dataset<Row> df_filtered_orders = completeAndClosedOrdersByDate(df_orders);
		Dataset<Row> df = df_filtered_orders.withColumn("month", date_format(df_filtered_orders.col("order_date"), "MM"))
		.join(df_customers, df_customers.col("customer_id").equalTo(df_filtered_orders.col("order_customer_id")));

		df =  df.groupBy(df.col("month"),df.col("customer_id"),
				df.col("first_name"), df.col("last_name"))
		.agg(count(lit(1)).as("count"));

		WindowSpec spec = Window.partitionBy(df.col("month"))
				.orderBy(df.col("count").desc());

		df = df.withColumn("rowNUmber", row_number().over(spec));

		df = df.filter(df.col("rowNumber").$less(4));

		df.show();

		// Write Output in CSV file format with out compression
		// Output directory should be passed as argument
		// Output should contain order_month, customer_id, customer_fname, customer_lname and revenue
		String outputFormat = "csv";
		writeOutputToLocal(spark, df, args[4], outputFormat);

		// Output should contain order_month, customer_id, customer_fname, customer_lname and revenue
		// Data should be sorted in ascending order by order_month and descending order by revenue

	}

	private static void writeOutputToLocal(SparkSession spark, Dataset<Row> df, String outputPath, String outputFormat) {

		df.coalesce(2).write().mode(SaveMode.Overwrite).option("header", true).format(outputFormat).save(outputPath);

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
