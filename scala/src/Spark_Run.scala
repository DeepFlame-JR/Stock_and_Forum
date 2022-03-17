import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Spark_Run extends App {
  // SparkContext 객체 초기화
  // 클러스터 매니저의 타입 지정
  val conf = new SparkConf().setAppName("spark").setMaster("local")
  val sc = new SparkContext(conf)
}


