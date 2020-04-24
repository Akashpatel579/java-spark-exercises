package df;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;


public class Q04GetProductRevenueForAGivenDay {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession
				.builder()
				.master(args[0])
				.appName("Get Product Revenue for a given day")
				.getOrCreate();
		
		// Read Orders, Order Items and Products Data using JSON data
		// Input base directory should be passed as argument

		String ordersSchema = "order_id INT, order_date STRING, order_customer_id INT, order_status STRING";

		String ordersItemsSchema = "order_item_id INT, order_item_order_id INT, order_item_product_id INT, " +
				"order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT";

		String productsSchema = "product_id INT, unknown INT, product_name STRING, unknown2 STRING," +
				" prodct_price FLOAT, image_path STRING";
		Dataset<Row> df_orders = readData(spark, args[1], "csv", ordersSchema);

		Dataset<Row> df_order_items = readData(spark, args[2], "csv", ordersItemsSchema);

		Dataset<Row> df_products = readData(spark, args[3], "csv", productsSchema);

//		df_orders.show();
//
//		df_orders.count();
//
//		df_order_items.show();
//
//		df_order_items.count();
//
//		df_products.show();
//		df_products.count();

		// Process using Data Frame Operations
		// Filter for complete or closed orders as well as the passed date
		// Get revenue for each product
		Dataset<Row> df_filtered_orders = completeAndClosedOrdersByDate(df_orders, args[4]);

//		df_filtered_orders.show();

		Dataset<Row> df_revenue_by_product = revenueByProduct(df_order_items);

		df_revenue_by_product.show();

		Dataset<Row> df_revenue_by_product_name = revenueByProductName(df_revenue_by_product, df_products);

//		df_revenue_by_product_name.show();


		// Write Output into Parquet file format with out compression
		// Output directory should be passed as argument
		// Output should contain Product Name and Revenue in descending order by revenue
		String outputFormat = "parquet";
		writeOutputToLocal(spark, df_revenue_by_product_name, args[5], outputFormat);



	}


	private static void writeOutputToLocal(SparkSession spark, Dataset<Row> df, String outputPath, String outputFormat) {

		df.coalesce(2).write().mode(SaveMode.Overwrite).format(outputFormat).save(outputPath);

	}
	private static Dataset<Row> revenueByProductName(Dataset<Row> df_revenue_by_product, Dataset<Row> df_products) {
		Dataset<Row> prepared_df = df_revenue_by_product
									.join(df_products,
											df_revenue_by_product.col("order_item_product_id")
													.equalTo(df_products.col("product_id")))
									.select(df_products.col("product_name"), df_revenue_by_product.col("Total_Revenue"))
									.orderBy(col("Total_Revenue").desc());

		return prepared_df;
	}

	private static Dataset<Row> revenueByProduct(Dataset<Row> df) {

		Dataset<Row> agg_df = df
							.select("order_item_product_id",
							"order_item_subtotal")
							.groupBy(df.col("order_item_product_id"))
								.agg(round(sum(df.col("order_item_subtotal")),2).as("Total_Revenue"));

		return agg_df;
	}


	private static Dataset<Row> completeAndClosedOrdersByDate(Dataset<Row> df, String date){

		String[] orderStatuses = new String[2];
		orderStatuses[0] = "COMPLETE";
		orderStatuses[1] = "CLOSED";


		System.out.println("date >> " + date);
		Dataset<Row> dff = df.filter(df.col("order_status").isin(orderStatuses)
				.and(df.col("order_date").equalTo(date)));
		return dff;
	}
	private static Dataset<Row> readData(SparkSession spark, String datasetPath, String datasetFormat, String tblSchema) {

		Dataset<Row> df= spark.read().format(datasetFormat).schema(tblSchema).load(datasetPath);

		return df;
	}

}
