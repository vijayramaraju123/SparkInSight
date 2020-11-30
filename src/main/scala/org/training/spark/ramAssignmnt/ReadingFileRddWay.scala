package org.training.spark.ramAssignmnt

import org.apache.spark.sql.SparkSession
import org.training.spark.ramAssignmnt.DataFrameCrreation.getClass

object ReadingFileRddWay {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(getClass.getName)
      .getOrCreate()


    var a= spark.sparkContext.textFile("C:\\Users\\Incredible\\Documents\\run\\ramAssignmnt\\product.txt")
    var a_1 = a.first()
    var b= spark.sparkContext.textFile("C:\\Users\\Incredible\\Documents\\run\\ramAssignmnt\\supplier.txt")
    var b_1 = a.first()
    var c= spark.sparkContext.textFile("C:\\Users\\Incredible\\Documents\\run\\ramAssignmnt\\products_suppliers.txt")
    var c_1 = a.first()


    import spark.implicits._

    var d = a.filter(x => x!=a_1).map(_.split(","))
    var e = b.filter(x => x!=b_1).map(_.split(","))
    var f = c.filter(x => x!=c_1).map(_.split(","))

    var df = d.map(a=>(a(0),a(1),a(2),a(3),a(4),a(5))).toDF("productID","productCode","name","quantity","price","supplierid")
    var df_1 = e.map(a=>(a(0),a(1),a(2))).toDF("supplierid","supplier_name","phone")
    var df_2 = f.map(a=>(a(0),a(1))).toDF("productID","supplierID")

    df.registerTempTable("product")
    df_1.registerTempTable("supplier")
    df_2.registerTempTable("product_supplier")


/*
    var df_3 = spark.sql("select p.productID,p.name,p.price,s.supplier_name,s.supplierid from product p inner join supplier s on p.supplierid == s.supplierid ")
    df_3.registerTempTable("table")
    var df_4 = spark.sql("select * from table where name = 'Pencil 3B'")
    var df_5 = spark.sql("select * from table where supplier_name = 'ABC Traders'")
   */


    var pr_1 = spark.sql("select product.name AS Product_Name, product.price,supplier.supplier_name AS Supplier_Name from product_supplier join product ON product_supplier.productID = product.productID JOIN supplier ON product_supplier.supplierID = supplier.supplierid ")
    var pr_2 = spark.sql("select supplier.supplier_name,product.name from product_supplier join product on product_supplier.productID = product.productID join supplier on product_supplier.supplierID = supplier.supplierid where product.name = 'Pencil 3B' ")
    var pr_3 = spark.sql("select supplier.supplier_name,product.name from product_supplier join product on product_supplier.productID = product.productID join supplier on product_supplier.supplierID = supplier.supplierid where supplier.supplier_name = 'ABC Traders' ")

 //   1. Select all the products which has product code as null

    var pr_4 = spark.sql("select productID,productCode,name from product where productCode is null ")

  //  2. Select all the products, whose name starts with Pen and results should be order by Price descending order.

    var pr_5 = spark.sql("select productCode,name,price from product where name like 'Pen %' order by price desc")

 //   3. Select all the products, whose name starts with Pen and results should be order by
 //   Price descending order and quantity ascending order.

    var pr_6 = spark.sql("select productCode,name,price,quantity from product where name like 'Pen %' order by quantity, price desc")


 //   4. Select top 2 products by price

    pr_5.registerTempTable("topProduct")

   // var pr_7 = spark.sql("select *  from topProduct order by quantity desc limit 2")

    var pr_7 = spark.sql("select *  from topProduct order by price desc limit 2")


    /*
    val df_order = sqlContext.read.parquet("/user/cloudera/Spark_Sql/orders/")

df_order.registerTempTable("orders")

var df_order_sql = sqlContext.sql("select * from orders limit 10")

df_order.registerTempTable("orders")

var df_order_sql = sqlContext.sql("select * from orders limit 10")

var df_order_sql = sqlContext.sql("select * from orders limit 10")

var df_order_sql = sqlContext.sql("select * from orders where order_status = 'CLOSED'")

var df_order_sql = sqlContext.sql("select * from orders where order_status in('CLOSED','COMPLETE')")

var df_order_sql = sqlContext.sql("select * from orders where order_date like '%2013%'")

var df_order_sql = sqlContext.sql("select * from orders where order_date like '%1374%'")

var df_order_sql = sqlContext.sql("select count(*) from orders where order_date like '%1374%'")

var df_order_sql = sqlContext.sql("select distinct(order_status) from orders ")

var df_order_sql = sqlContext.sql("select order_status,count(order_status) from orders group by order_status ")

var df_order_sql = sqlContext.sql("select order_status,count(order_status) as order_status_count from orders group by order_status ")

var df_order_sql = sqlContext.sql("select order_status,count(order_status) as order_status_count from orders group by order_status order by order_status_count")

// joins
Joining:
val df= sqlContext.read.parquet("/user/cloudera/Spark_Sql/customers/")

df.registerTempTable("customers")

val df_join = sqlContext.sql("select o.*,c.* from orders o inner join customers c on o.order_customer_id =c.customer_id")

val df_left_join = sqlContext.sql("select o.*,c.* from orders o left outer join customers c on o.order_customer_id =c.customer_id")

val df_right_join = sqlContext.sql("select o.*,c.* from orders o right outer join customers c on o.order_customer_id =c.customer_id")

Window Functions:

 select * from(select o.order_id,o.order_date,o.order_status,oi.order_item_subtotal,
 round(sum(order_item_subtotal) over(partition by o.order_id),2) order_revenue ,
 order_item_subtotal/round(sum(order_item_subtotal) over(partition by order_id ),2) pct_revenue,
 round(avg(order_item_subtotal) over(partition by o.order_id),2) avg_revenue,
 rank() over (partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
 dense_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
 percent_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) percent_rnk_revenue,
 row_number() over (partition by o.order_id order by oi.order_item_subtotal desc) rn_revenue,
 row_number() over (partition by o.order_id) order_rn_revenue
 from orders o join order_items oi on o.order_id = oi.order_item_order_id
 where o.order_status in('COMPLETE','CLOSED')) sample1 where order_revenue >1000 order by order_date,order_revenue desc,rnk_revenue;


     */



  }

}
