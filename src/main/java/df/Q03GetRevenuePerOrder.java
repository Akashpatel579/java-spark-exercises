package df;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.count;

public class Q03GetRevenuePerOrder {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession
				.builder()
				.master(args[0])
				.appName("Get Revenue Per Order")
				.getOrCreate();
		
		// Read Order Items Data
		// Input base directory should be passed as argument
		String orderSchema = "order_id INT, order_date STRING, order_item_order_id INT, order_status STRING";
		Dataset<Row> df = readData(spark, args[1], orderSchema);
		df.show();
		
		// Process using Data Frame Operations
		// Aggregate using order_item_order_id as key
		Dataset<Row> aggOrderItemByOrderId = aggCountOrderItemByOrderId(df);
		aggOrderItemByOrderId.show();

		// Write Output into JSON Files
		// Output directory should be passed as argument
		// Output should contain order_id and order_revenue
		writeOutputToLocal(spark, aggOrderItemByOrderId, args[2]);


	}

	private static void writeOutputToLocal(SparkSession spark, Dataset<Row> dff, String outputPath) {

		spark.conf().set("spark.sql.shuffle.partitions", 2);

		dff.write().mode(SaveMode.Overwrite).format("csv").option("header", true).save(outputPath);
	}


	private static Dataset<Row> aggCountOrderItemByOrderId(Dataset<Row> df) {


//		Map<String,String> map=new HashMap<String, String>();
//		map.put("order_item_order_id","max");
//
//		return df.groupBy(df.col("order_item_order_id"))
//				.agg(map);

		return df.groupBy(df.col("order_item_order_id"))
				.agg(count(df.col("order_item_order_id")).as("agg_order_item_count"));

	}

	private static Dataset<Row> readData(SparkSession spark, String datasetPath, String orderSchema) {


		Dataset<Row> df = spark.read().format("csv").schema(orderSchema)
				.load(datasetPath);

		return  df;
	}

}
