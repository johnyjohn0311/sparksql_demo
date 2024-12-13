package sparksql_demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class TestSparkSql {
	@Test
	public void testSparkSql() {
		SparkSession spark = SparkSession.builder()
				.appName("Part-1")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> data = spark.read()
				.option("inferSchema", true)
				.option("header", true)
				.csv("D:\\eclipse-workspace\\sparksql_demo\\src\\test\\java\\sparksql_demo\\retails.csv");
		
		data.show();
        // Lấy ra số lượng khách hàng
		long cntCustomers1 = data.select("CustomerID").count();
		
		// Đếm số khách hàng không có thông tin 
		long cntCustomersNoInfor = data.select("CustomerID").filter(data.col("CustomerID").isNull()).count();

		// Số lượng khách hàng khác nhau
		// trừ 1 vì có một giá trị null trong thông tin của customer ID
		long cntCustomers = data.select("CustomerID").distinct().count() - 1; 
		
		// Số sản phẩm khác nhau
		long cntProdcts = data.select("StockCode").distinct().count();
		
		// Số giao dịch khác nhau
		long cntInvoices = data.select("InvoiceNo").distinct().count();
		
		// In ra kết quả
		System.out.println("Số lượng khách hàng:" + cntCustomers1);
		System.out.println("Số lượng khách hàng không có thông tin:" + cntCustomersNoInfor);
		System.out.println("Số khách hàng khác nhau: " + cntCustomers); 
		System.out.println("Số sản phẩm khác nhau: " + cntProdcts);
		System.out.println("Số giao dịch khác nhau: " + cntInvoices);
		
		// output: 
		// Số khách hàng khác nhau: 4372
		// Số sản phẩm khác nhau: 4070`
		// Số giao dịch khác nhau: 25900

		// Chọn ra tổng số hàng hoá của mỗi nước và xếp theo số lượng giảm dần
		data.createOrReplaceTempView("data");
		spark.sql("select Country, sum(Quantity) as count from data group by Country order by count desc").show();
	}
}


