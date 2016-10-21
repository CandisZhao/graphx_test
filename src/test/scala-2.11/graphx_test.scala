import java.io.{File, PrintWriter}

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.compat.Platform._
import scala.io.Source

object graphx_test{


  def main(args: Array[String]): Unit = {
    //
    val executionStart: Long = currentTime
    val g = construct_graph()
    xJumpsFriends(g, 2)
    val total = currentTime - executionStart
    Console.println("[total " + total + "ms]")
  }

  def readFiletoHBase(): Unit = {
    val executionStart: Long = currentTime

    val sparkConf = new SparkConf().setAppName("HBaseRead").setMaster("local").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val table_name = "connection_1019"
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.rootdir", "file:///usr/local/var/hbase");
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, table_name)
    val hadmin = new HBaseAdmin(conf)
    if (!hadmin.isTableAvailable(table_name)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(table_name)
      tableDesc.addFamily(new HColumnDescriptor("mobile".getBytes()))
      hadmin.createTable(tableDesc)
    }else{
      print("Table  Exists!  not Create Table")
    }

    val table = new HTable(conf, table_name);

    val fin = "/Users/canzhao/Desktop/canzh_program/loan_user_connection0919/loan_user_connection.txt"
    for (line <- Source.fromFile(fin,"UTF-8").getLines()){
      val s = line.split("\t")
      //去掉自环的操作
      if(Bytes.toBytes(s(1)).equals(Bytes.toBytes(s(2)))){
      }
      else{
        var tmp = new Put(Bytes.toBytes(s(1)))
        tmp.add(Bytes.toBytes("mobile"), Bytes.toBytes(s(2)),Bytes.toBytes("value " + 1))
        table.put(tmp)
      }
      //保留自环的话
//      var tmp = new Put(Bytes.toBytes(s(1)))
//      tmp.add(Bytes.toBytes("mobile"), Bytes.toBytes(s(2)),Bytes.toBytes("value " + 1))
//      table.put(tmp)
    }
    table.flushCommits()

    //scan操作
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]);
    hbaseRDD.cache()
    println("------------------------scan----------------")
    val res = hbaseRDD.take(10)
    for (j <- 1 until 11) {
      println("j: " + j)
      var rs = res(j - 1)._2
      var kvs = rs.raw
      for (kv <- kvs)
        println("rowkey:" + new String(kv.getRow()) +
          " cf:" + new String(kv.getFamily()) +
          " column:" + new String(kv.getQualifier()) +
          " value:" + new String(kv.getValue()))
    }
    println("-------------------------")

    sc.stop()

    val total = currentTime - executionStart
    Console.println("[total " + total + "ms]")
  }

  def readLabeltoHBase(): Unit = {
    val executionStart: Long = currentTime

    val sparkConf = new SparkConf().setAppName("HBaseRead").setMaster("local").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val table_name = "connection_label_1019"
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.rootdir", "file:///usr/local/var/hbase");
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, table_name)
    val hadmin = new HBaseAdmin(conf)
    if (!hadmin.isTableAvailable(table_name)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(table_name)
      tableDesc.addFamily(new HColumnDescriptor("label".getBytes()))
      hadmin.createTable(tableDesc)
    }else{
      print("Table  Exists!  not Create Table")
    }
    val table = new HTable(conf, table_name);

    val fin_guest = "/Users/canzhao/Desktop/canzh_program/loan_user_connection0919/guest_mobile_number.txt"
    for (line <- Source.fromFile(fin_guest,"UTF-8").getLines()) {
      val s = line.split("\t")
      var tmp = new Put(Bytes.toBytes(s(0)))
      tmp.add(Bytes.toBytes("label"), Bytes.toBytes(s(1)), Bytes.toBytes("value " + 1))
      table.put(tmp)
    }
    val fin_host = "/Users/canzhao/Desktop/canzh_program/loan_user_connection0919/host_mobile_number.txt"
    for (line <- Source.fromFile(fin_host,"UTF-8").getLines()){
      val s = line.split("\t")
      var tmp = new Put(Bytes.toBytes(s(0)))
      tmp.add(Bytes.toBytes("label"), Bytes.toBytes(s(1)),Bytes.toBytes("value " + 1))
      table.put(tmp)
    }
    table.flushCommits()

    sc.stop()
    val total = currentTime - executionStart
    Console.println("[total " + total + "ms]")
  }

  def construct_graph(): Graph[String,None.type ] = {
    val executionStart: Long = currentTime

    val sconf = new SparkConf()
      .setMaster("local")
      .setAppName("Spark Graphx")
      .set("spark.executor.memory", "1g").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sconf)

    //第一张表connection的
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.rootdir", "file:///usr/local/var/hbase");
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val table_name = "connection_1019"
    conf.set(TableInputFormat.INPUT_TABLE, table_name)
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //取边
    val lt: ListBuffer[(Long, String)]  = ListBuffer();
    hbaseRDD.collect().foreach(row => {
      var it = row._2.getFamilyMap("mobile".getBytes).keySet().iterator()
      while (it.hasNext()) {
        var pair = (new String(row._2.getRow()).toLong, new String(it.next()));
        lt.+=(pair);
      }
    })

    //EdgeRDD
    val edge = sc.parallelize(lt.toList).map(x=>{
      Edge(x._1.toLong,x._2.toLong,None)
    })

    //第二张表节点的label信息
    val conf_v = HBaseConfiguration.create()
    conf_v.set("hbase.zookeeper.quorum", "localhost");
    conf_v.set("hbase.rootdir", "file:///usr/local/var/hbase");
    conf_v.set("hbase.zookeeper.property.clientPort", "2181")
    val table_name_v = "connection_label_1019"
    conf_v.set(TableInputFormat.INPUT_TABLE, table_name_v)
    val hbaseRDD_v = sc.newAPIHadoopRDD(conf_v, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //取点
    val lt_v: ListBuffer[(Long, String)]  = ListBuffer();

    hbaseRDD_v.collect().foreach(row => {
      var it = row._2.getFamilyMap("label".getBytes).keySet().iterator()
      while (it.hasNext()) {
        var pair = (new String(row._2.getRow()).toLong, new String(it.next()));
        lt_v.+=(pair);
      }
    })

    //VertexRDD
    val vertex = sc.parallelize(lt_v.toList).map(x=>{
      (x._1.toLong,x._2.toString)
    })

    val g = Graph(vertex, edge)
    g
  }

  def xJumpsFriends(g:Graph[String, None.type], n:Int): Unit = {
    type VMap=Set[VertexId]
    def vprog(vid:VertexId,vdata:VMap,message:VMap)
    :Set[VertexId]=addMaps(vdata,message)

    def sendMsg(e:EdgeTriplet[VMap, _])= {
      Iterator((e.srcId, e.dstAttr), (e.dstId, e.srcAttr))
    }
    def addMaps(spmap1: VMap, spmap2: VMap): VMap =
      (spmap1 ++ spmap2)

    val two=n  //这里是n跳邻居 所以2跳的话只需要定义为2即可
    val newG=g.mapVertices((vid,_)=>Set[VertexId](vid))// init vertex, self set only contain self
      .pregel(Set[VertexId](), two, EdgeDirection.In)(vprog, sendMsg, addMaps)

    //打印前10个的节点的n跳邻居信息
    newG.vertices.collect().take(10).foreach(println(_))

    val nodes = g.vertices.map {
      x => x._1 -> x._2
    }.collectAsMap()

    var i = 0
    val data = newG.vertices.collect()

    val valueList: ListBuffer[(VertexId, Seq[Int])] = ListBuffer()
    val writer = new PrintWriter(new File("/Users/canzhao/Desktop/canzh_program/loan_user_connection0919/output_test.txt"))
    val str:String = "mobile_number,N,I,A,R,L,M,null\n"
    writer.write(str) //写head标签
    for(i <- 0 until data.length){
      //未授信
      val temp_N = data(i)._2.count(x => {
        nodes.get(x) == Some("N")
      })
      val temp_I = data(i)._2.count(x => {
        nodes.get(x) == Some("I")
      })
      val temp_A = data(i)._2.count(x => {
        nodes.get(x) == Some("A")
      })
      val temp_R = data(i)._2.count(x => {
        nodes.get(x) == Some("R")
      })
      val temp_L = data(i)._2.count(x => {
        nodes.get(x) == Some("L")
      })
      val temp_M = data(i)._2.count(x => {
        nodes.get(x) == Some("M")
      })
      val temp_null = data(i)._2.count(x => {
        nodes.get(x) == Some(null)
      })
      val label = Seq(temp_N, temp_I, temp_A, temp_R, temp_L, temp_M, temp_null)

      val pair = (data(i)._1, label);
      val line = data(i)._1 + "," + label.mkString(",")
      writer.write(line)
      writer.write("\n")
      valueList.+=(pair)
    }
    writer.close()
  }
}


