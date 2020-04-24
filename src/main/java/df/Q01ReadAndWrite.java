package df;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Q01ReadAndWrite {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark = SparkSession
				.builder()
				.master("local")
				.appName("Read and Write APIs")
				.getOrCreate();

		System.out.println("Hello Spark Application!");

	}

}
