package df;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;


import static org.apache.spark.sql.functions.*;


public class Q05GetRevenueForFirstDayOfEveryMonth {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession
				.builder()
				.master(args[0])
				.appName("Read and Write APIs")
				.getOrCreate();
		// Read Orders as well as Order Items
		// Input base directory should be passed as argument
		String ordersSchema = "order_id INT, order_date STRING, order_customer_id INT, order_status STRING";

		String ordersItemsSchema = "order_item_id INT, order_item_order_id INT, order_item_product_id INT, " +
				"order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT";

		Dataset<Row> df_orders = readData(spark, args[1], "csv", ordersSchema);

		Dataset<Row> df_order_items = readData(spark, args[2], "csv", ordersItemsSchema);

		df_orders.show();

		df_order_items.show();

		// Process using Data Frame Operations
		// Filter for complete or closed orders as well as 1st of every month
		// Compute revenue for each filtered date.
		Dataset<Row> df_revenue_by_datas = revenueByFirstDateOfMonth(df_orders, df_order_items);

		df_revenue_by_datas = getFirstOrderOfEveryMonth(df_revenue_by_datas);

		df_revenue_by_datas.show();

		// Write Output into ORC file format with out compression
		// Output directory should be passed as argument
		// Output should contain Date and Revenue sorted by date
		String outputFormat = "orc";
		writeOutputToLocal(df_revenue_by_datas, args[3], outputFormat);

	}


	private static void writeOutputToLocal(Dataset<Row> df, String outputPath, String outputFormat) {

		df.coalesce(2).write().mode(SaveMode.Overwrite).format(outputFormat).save(outputPath);

	}
	private static Dataset<Row> getFirstOrderOfEveryMonth(Dataset<Row> df_revenue_by_datas) {

		df_revenue_by_datas = df_revenue_by_datas.withColumn("yearMM", date_format(df_revenue_by_datas.col("order_date"),
				"yyyyMM"));

		WindowSpec spec = Window.partitionBy(df_revenue_by_datas.col("yearMM"))
				.orderBy(df_revenue_by_datas.col("yearMM"));

		df_revenue_by_datas.printSchema();

		df_revenue_by_datas = df_revenue_by_datas.withColumn("rowNUmber", row_number().over(spec));

		return df_revenue_by_datas.select(df_revenue_by_datas.col("order_date"),
						df_revenue_by_datas.col("total_revenue"),
						df_revenue_by_datas.col("rowNUmber"))
				.where(df_revenue_by_datas.col("rowNUmber").equalTo(1))
				.orderBy(df_revenue_by_datas.col("order_date"));


	}

	private static Dataset<Row> revenueByFirstDateOfMonth(Dataset<Row> df_orders, Dataset<Row> df_order_items) {

		Dataset<Row> prepared_df = df_order_items
				.join(df_orders, df_orders.col("order_id").equalTo(df_order_items.col("order_item_order_id")))
				.select(df_orders.col("order_date"), df_order_items.col("order_item_subtotal"))
				.groupBy(df_orders.col("order_date"))
				.agg(sum(df_order_items.col("order_item_subtotal")).as("total_revenue"));
		return  prepared_df;
	}

	private static Dataset<Row> readData(SparkSession spark, String datasetPath, String datasetFormat, String tblSchema) {

		Dataset<Row> df= spark.read().format(datasetFormat).schema(tblSchema).load(datasetPath);

		return df;
	}

}
