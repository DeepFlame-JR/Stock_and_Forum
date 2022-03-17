import org.apache.spark.{SparkConf, SparkContext}

object Base extends App{
  // SparkContext 객체 초기화
  // 클러스터 매니저의 타입 지정
  val conf = new SparkConf().setAppName("spark").setMaster("local")
  val sc = new SparkContext(conf)

  val data = Array(1, 2, 3, 4, 5)
  val distData = sc.parallelize(data)
  var re = distData.reduce((a, b) => a + b)
  println(re)

}