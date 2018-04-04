import org.apache.spark._
import org.apache.hadoop.fs._
import math._


object KNN {

    def distance(xs: Array[Double], ys: Array[Double]) = {
        sqrt((xs zip ys).map { case (x,y) => pow(y - x, 2) }.sum)
       }


    def main(args: Array[String]){
        val files = args(0)
        val id = args(1).toInt
        val k = args(2).toInt

        val conf = new SparkConf().setAppName("KNNNNNNNNNNN").set("spark.executor.memory", "20g")
        val sc = new SparkContext(conf)



        println("\n Main function !!!!!!")

        println(" ")

        val data = sc.textFile(files).map(line =>line.split(',')).map(elems => (elems(0).toInt,elems.slice(1,19).map(_.toDouble),elems(19).toDouble))

        val input = data.take(id+1)(id)._2

        println("Your Input Id is : "+ id + "\n Your Input K is :" + k )

        println("")

        val Point_distance = data.map(d=>(d._1,distance(d._2,input),d._3))
        val Find_K_near = Point_distance.filter(_._1 != id).sortBy(_._2).take(k)
        Find_K_near.foreach(println)

        print("-----------------------------------")

        val labelCount = sc.parallelize(Find_K_near).map(points =>(points._3,1)).reduceByKey(_+_).sortBy(_._2,false).first
        println("")
        println("Predict Result : " + labelCount._1)
        println("Probability : " + labelCount._2.toDouble / k)

        println("")

        println("Real result : "+ data.take(id+1)(id)._3.toInt.toString )

        println("")
        println("\n End !!!!!")


        sc.stop
     }
}
