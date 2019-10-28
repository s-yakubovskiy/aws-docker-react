package ru.croc.gpn_mo

import java.time.LocalDateTime

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{struct, _}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


object PIStreaming2 {

  @transient lazy private val log = Logger.getLogger(getClass.getName)

  private val INPUT_TOPIC_NAME = "pi_input_tpc"
  private val OUTPUT_CONVERT_TOPIC_NAME = "pi_value_convert_tpc"
  private val OUTPUT_WARNING_TOPIC_NAME = "pi_warning_tpc"
  private val OUTPUT_RANGE_TOPIC_NAME = "pi_range_output_tpc"
  private val RANGE_SNAPSHOT_TOPIC_NAME = "pi_range_snapshot_tpc"

  private val REQUIREMENTS_TBL_PATH = "/smotr/gpnmo/1c/pi/"
  private val WORK_REQUIREMENTS_PATH = "/smotr/requirements/pi"
  private val CHECKPOINT_DIR_PATH = "/smotr/pi-checkpoints"
  private val RANGE_SNP_TBL_PATH = "/smotr/pi-range_snp"
  private val OLD_RANGE_SNP_TBL_PATH = "/smotr/pi_old_range_snp"

  private val RANGE_SNP_TBL_FORMAT = "com.databricks.spark.avro"
  private val STREAM_TRIGGER_TIMEOUT = 240
  private val STREAM_WATERMARK = 30
  private val STREAM_GROUP_DEF = "pi-streaming"
  private var STREAM_GROUP = STREAM_GROUP_DEF


  private val DEBUG_DF_PATH = "/smotr/df_debug"

//todo добавить if если приложение запущено в режиме debug
//  private val INPUT_TOPIC_NAME = "pi_test_input_tpc"
//  private val OUTPUT_CONVERT_TOPIC_NAME = "pi_test_value_convert_tpc"
//  private val OUTPUT_WARNING_TOPIC_NAME = "pi_test_warning_tpc"
//  private val OUTPUT_RANGE_TOPIC_NAME = "pi_test_range_output_tpc"
//  private val RANGE_SNAPSHOT_TOPIC_NAME = "pi_test_range_snapshot_tpc"
//  private val REQUIREMENTS_TBL_PATH = "/smotr/gpnmo/1c/pi/"
//  private val WORK_REQUIREMENTS_PATH = "/smotr/requirements/pi_debug"
//  private val CHECKPOINT_DIR_PATH = "/smotr/pi-checkpoints-debug"
//  private val RANGE_SNP_TBL_PATH = "/smotr/pi-range_snp_debug"
//  private val OLD_RANGE_SNP_TBL_PATH = "/smotr/pi_old_range_snp_debug"



  //incoming message schema
  val input_topic_schema = (new StructType)
    .add("tag_name", StringType, false)
    .add("tag_value", StringType, false)
    .add("unit", StringType, false)
    .add("date_request", StringType, false)
    .add("date_response", StringType, false)
    .add("status", IntegerType, true)
    .add("refinery_id", StringType, false)

  val range_snp_schema = (new StructType)
    .add("key", StringType, false)
    .add("refinery_id", IntegerType, false)
    .add("tag_name", StringType, false)
    .add("date_start", StringType, true)
    .add("top_difference", StringType, true)
    .add("event_code", IntegerType, false)
    .add("difference", StringType, true)
    .add("workmode", StringType, true)
    .add("date_request", StringType, true)
    .add("top_value", StringType, true)

  //hbase schemas    tag_name, refinery_id, event_code, date_start, top_difference, top_value
  val range_snp_catalog = s"""{
                             |"table":{"namespace":"default", "name":"range_snp_tbl"},
                             |"rowkey":"key",
                             |"columns":{
                             |"key":{"cf":"rowkey", "col":"key", "type":"string"},
                             |"tag_name":{"cf":"cf", "col":"tag_name", "type":"string"},
                             |"refinery_id":{"cf":"cf", "col":"refinery_id", "type":"integer"},
                             |"event_code":{"cf":"cf", "col":"event_code", "type":"integer"},
                             |"date_start":{"cf":"cf", "col":"date_start", "type":"string"},
                             |"top_difference":{"cf":"cf", "col":"top_difference", "type":"string"},
                             |"top_value":{"cf":"cf", "col":"top_value", "type":"string"}
                             |}
                             |}""".stripMargin

  var sqlDebug:String = _


  def main(args: Array[String]): Unit = {
    val argsMap = argsToOptionMap(args)
    if(args.length == 0 || argsMap.contains("h") || argsMap.contains("help")) {
      println("Usage: spark-submit ... --class ru.croc.gpn_mp.PIStreaming2 [--option arg]")
      println("Valid options:")
      println("--brokers [string]\r\n" +
        "--pathRequirements [string]\r\n" +
        "--pathCheckpoints [string]\r\n" +
        "--interval [int]\r\n\r\n" +
        "--topicInput [string]\r\n" +
        // "--topicInputUnion [string]\r\n" +
        "--topicOutputRange [string]\r\n" +
        "--topicOutputRangeSnp [string]\r\n" +
        "--topicOutputWarning [string]\r\n" +
        "--topicOutputConvert [string]\r\n" +
        "--fillWarning [true|false]\r\n" +
        "--fillConvert [true|false]\r\n" +
        "--fillRangeOut [true|false]\r\n" +
        "--fillRangeSnp [true|false]\r\n" +
        "--pathRangeSnp [string]\r\n" +
        "--formatRangeSnp [string]\r\n" +
        "--kafkaGroup [string]\r\n")
    }
    if(!argsMap.contains("brokers")) {
      println("No kafka-brokers, should be specified in --brokers")
      return
    }

    sqlDebug = argsMap.getOrElse("sql", null)
    val kafkaBrokers: String = argsMap("brokers")
    val requirementsPath: String = argsMap.getOrElse("pathRequirements", REQUIREMENTS_TBL_PATH)
    val workRequirementsPath: String = argsMap.getOrElse("pathWorkRequirements", WORK_REQUIREMENTS_PATH)
    val checkpointDir: String = argsMap.getOrElse("pathCheckpoints", CHECKPOINT_DIR_PATH)
    val streamInterval:Long = argsMap.getOrElse("interval", "" + STREAM_TRIGGER_TIMEOUT).toInt
    val rangeSnpPath: String = argsMap.getOrElse("pathRangeSnp", RANGE_SNP_TBL_PATH)
    val oldRangeSnpPath: String = argsMap.getOrElse("pathOldRangeSnp", OLD_RANGE_SNP_TBL_PATH)
    val rangeSnpFormat:String = argsMap.getOrElse("formatRangeSnp", RANGE_SNP_TBL_FORMAT)
    val topicInput:String = argsMap.getOrElse("topicInput", INPUT_TOPIC_NAME)
    STREAM_GROUP = argsMap.getOrElse("kafkaGroup", STREAM_GROUP_DEF)

    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf, Seconds(streamInterval))
    ssc.remember(Seconds(streamInterval))
    val spark = SparkSessionSingleton.getInstance(sparkConf)
    spark.sparkContext.setCheckpointDir(checkpointDir)
    import spark.implicits._

    //spark.createDataFrame(ssc.sparkContext.emptyRDD[Row], range_snp_schema).createOrReplaceTempView("range_snp")
    //convertStreamToDf(getDStream(RANGE_SNAPSHOT_TOPIC_NAME, kafkaBrokers, ssc), "range_snp", range_snp_schema, spark)

    val input = getDStream(topicInput, kafkaBrokers, ssc)
    input.foreachRDD(rddRaw => {

      val CUR_TIMESTAMP = java.time.LocalDateTime.now
      val conf = rddRaw.context.getConf
      val df = convertFromJson(rddRaw, input_topic_schema)
      df.createOrReplaceTempView("input")

      val cnt1 = df.count()
      //val cnt2 = spark.sql("select * from range_snp").count()
      if(cnt1 != 0) {
        loadRequirementsToWorkDir(requirementsPath, workRequirementsPath, sparkConf)
        val active_requirements_tbl = processRequirementsTable(workRequirementsPath+"/work", sparkConf)
        active_requirements_tbl.persist(StorageLevel.MEMORY_ONLY)
        // load range snapshots
        val range_snp_tbl:DataFrame = rangeSnpFormat match {
          case "parquet" | "com.databricks.spark.avro" => processRangeSnapshotTable(rangeSnpFormat, rangeSnpPath, oldRangeSnpPath, sparkConf)
          case "hbase" => fromHBase(spark, "range_snp", range_snp_catalog, emptyToNull = Seq("date_start", "top_difference", "top_value"))
            .selectExpr("key", "tag_name", "refinery_id", "event_code", "date_start", "cast(top_difference as decimal(18, 8)) as top_difference", "cast(top_value as decimal(18, 8)) as top_value")
          case _ => throw new RuntimeException("Unsupported '" + rangeSnpFormat + "' format for range_snp")
        }
        range_snp_tbl.persist(StorageLevel.MEMORY_ONLY)

        val df_input = processInput(df, active_requirements_tbl, conf)
        val df_full = processJoins(conf)

        val df_warning = processWarningTopic(df_full, active_requirements_tbl, conf, CUR_TIMESTAMP)
        if(argsMap.getOrElse("fillWarning", "true").toBoolean) {
          try {
            toKafka(
              kafkaBrokers,
              argsMap.getOrElse("topicOutputWarning", OUTPUT_WARNING_TOPIC_NAME),
              checkpointDir,
              df_warning.select(to_json(struct('tag_name, 'date, 'metrics_value, 'event_code, 'refinery_id, 'difference, 'flag)) as 'value)
            ).save()
          } catch {
            case e:Exception => System.out.println("Error on saving topicOutputWarning: " + e.getMessage);
          }
        }

        //val df_range_snp = processRangeTopic(conf)
        /*toKafka(
        kafkaBrokers,
        argsMap.getOrElse("topicOutputRangeSnp", RANGE_SNAPSHOT_TOPIC_NAME),
        checkpointDir,
        processRangeTopic(conf)
          .select('key, to_json(struct('tag_name, 'refinery_id, 'event_code, 'date_start, 'top_difference, 'top_vae)) as 'value)
      ).save()*/

        val df_range = processRangeOutput(conf, CUR_TIMESTAMP)
        df_range.createOrReplaceTempView("out_range_output")
//        System.out.println("df_range")
//        df_range.filter("id_metrics in ('AR10:LRC29')").show()
        if(argsMap.getOrElse("fillRangeOut", "true").toBoolean) {
          try {
            toKafka(
              kafkaBrokers,
              argsMap.getOrElse("topicOutputRange", OUTPUT_RANGE_TOPIC_NAME),
              checkpointDir,
              df_range
                //              .withColumn("metrics_value",format_number($"metrics_value",5))
                .withColumn("metrics_value_tmp", df_range.col("metrics_value").cast(DecimalType(18,5))).drop("metrics_value").withColumnRenamed("metrics_value_tmp","metrics_value")
                .select(to_json(struct('refinery_id, 'id_metrics, 'date_start, 'date_end, 'event_code, 'metrics_value, 'difference, 'workmode)) as 'value)
            ).save()
          } catch {
            case e:Exception => System.out.println("Error on saving topicRangeOutput " + e.getMessage);
          }
        }

        val df_converted = processConvertTopic(conf)
        df_converted.createOrReplaceTempView("out_value_convert")
        if(argsMap.getOrElse("fillConvert", "true").toBoolean) {
          try {
            toKafka(
              kafkaBrokers,
              argsMap.getOrElse("topicOutputConvert", OUTPUT_CONVERT_TOPIC_NAME),
              checkpointDir,
              df_converted
                //              .withColumn("tag_value",format_number($"tag_value",5))
                .withColumn("tag_value_tmp", df_converted.col("tag_value").cast(DecimalType(18,5))).drop("tag_value").withColumnRenamed("tag_value_tmp","tag_value")
                .select(to_json(struct('tag_name, 'tag_value, 'unit, 'date_request, 'date_response, 'status, 'refinery_id)) as 'value)
            ).save()
          } catch {
            case e:Exception => System.out.println("Error on saving topicOutputConvert: " + e.getMessage);
          }
        }

        // save range snapshots
        val df_range_snp = processRangeTopic(conf, CUR_TIMESTAMP)
        df_range_snp.cache()
        df_range_snp.createOrReplaceTempView("out_range_snp")
//        System.out.println("df_range_snp")
//        df_range_snp.filter("tag_name in ('ISO:FI305.F')").show()
        if(argsMap.getOrElse("fillRangeSnp", "true").toBoolean) {
          val cnt2 = df_range_snp.count()
          if(cnt2 != 0) {
            rangeSnpFormat match {
              case "parquet" | "com.databricks.spark.avro" =>
                toFile(df_range_snp, rangeSnpFormat, rangeSnpPath)
                toFile(df_range_snp, rangeSnpFormat, oldRangeSnpPath)
                range_snp_tbl.unpersist()
              case "hbase" => toHBase(
//                TODO: add workmode if hbase
                df_range_snp
                  .selectExpr("key", "tag_name", "refinery_id", "event_code", "date_start", "cast(top_difference as varchar(100)) as top_difference", "cast(top_value as varchar(100)) as top_value")
                  .na.fill("", Seq("date_start", "top_difference", "top_value")), range_snp_catalog)
              case "kafka" => toKafka(
                kafkaBrokers,
                argsMap.getOrElse("topicOutputRangeSnp", RANGE_SNAPSHOT_TOPIC_NAME),
                checkpointDir,
                df_range_snp.select('key, to_json(struct('tag_name, 'refinery_id, 'event_code, 'date_start, 'top_difference, 'top_value)) as 'value)
              ).save()
              case _ => throw new RuntimeException("Unsupported '" + rangeSnpFormat + "' format for range_snp")
            }
          }
        }
        toFile(active_requirements_tbl, "com.databricks.spark.avro", workRequirementsPath+"/old")
        active_requirements_tbl.unpersist()
        df_range_snp.unpersist()

        if(sqlDebug != null) {
          spark.sql(sqlDebug).show(50, false)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def argsToOptionMap(args:Array[String]):Map[String,String]= {
    def nextOption(
                    argList:List[String],
                    map:Map[String, String]
                  ) : Map[String, String] = {
      val pattern       = "--(\\w+)".r // Selects Arg from --Arg
      val patternSwitch = "-(\\w+)".r  // Selects Arg from -Arg
      argList match {
        case Nil => map
        case pattern(opt)       :: value  :: tail => nextOption( tail, map ++ Map(opt -> value) )
        case patternSwitch(opt) :: tail => nextOption( tail, map ++ Map(opt -> null) )
        case string             :: Nil  => map ++ Map(string -> null)
        case option             :: tail => {
          println("Unknown option:" + option)
          sys.exit(1)
        }
      }
    }
    nextOption(args.toList,Map())
  }


  def loadFileTable(format:String, conf:SparkConf, path:String, viewName:String, sqlString:String = null, sqlViewName:String = null): DataFrame = {
    val spark = SparkSessionSingleton.getInstance(conf)
    val tbl = spark.read.format(format).load(path)
    val lower_tbl = tbl.toDF(tbl.columns map(_.toLowerCase): _*)
    lower_tbl.createOrReplaceTempView(viewName)
    if(sqlString != null) {
      val res_tbl = spark.sql(sqlString).distinct()
      res_tbl.createOrReplaceTempView(sqlViewName)
      res_tbl
    } else {
      lower_tbl
    }
  }

  def processRequirementsTable(requirementsTblPath:String, conf:SparkConf): DataFrame = {
    loadFileTable("com.databricks.spark.avro", conf, requirementsTblPath, "lower_requirements_tbl", "select * from lower_requirements_tbl where activity == 'true'", "active_requirements_tbl")
  }

  def loadRequirementsToWorkDir(requirementsPath:String, workRequirementsPath:String, conf:SparkConf): Unit = {
    /*val spark = SparkSessionSingleton.getInstance(conf)
    val requirements_tbl = spark.read.format("com.databricks.spark.avro").load(requirementsTblPath)
    val lower_requirements_tbl = requirements_tbl.toDF(requirements_tbl.columns map(_.toLowerCase): _*)
    lower_requirements_tbl.createOrReplaceTempView("lower_requirements_tbl")
    val active_requirements_tbl = spark.sql("select * from lower_requirements_tbl where activity == 'true'").distinct()
    active_requirements_tbl.createOrReplaceTempView("active_requirements_tbl")
    active_requirements_tbl*/

    var df:DataFrame = null
    try {
      df = loadFileTable("com.databricks.spark.avro", conf, requirementsPath, "lower_requirements_tbl", "select * from lower_requirements_tbl where activity == 'true'", "active_requirements_tbl")
      toFile(df, "com.databricks.spark.avro", workRequirementsPath+"/work")
      System.out.println(LocalDateTime.now() + " Copy requirements from 1C with row count :" + df.count())
    } catch {
      case e:Exception => {
        df = loadFileTable("com.databricks.spark.avro", conf, workRequirementsPath+"/old", "lower_requirements_tbl", "select * from lower_requirements_tbl where activity == 'true'", "active_requirements_tbl")
        toFile(df, "com.databricks.spark.avro", workRequirementsPath+"/work")
        System.out.println(LocalDateTime.now() + " Copy requirements from old requirements with row count :" + df.count())
      }
    }
  }

  def processRangeSnapshotTable(format:String, rangeSnpPath:String, oldRangeSnpPath:String, conf:SparkConf): DataFrame = {
    val spark = SparkSessionSingleton.getInstance(conf)
    var df:DataFrame = null
    try {
      df = loadFileTable(format, conf, rangeSnpPath, "range_snp")

      System.out.println(LocalDateTime.now() + " Range snapshot read with row count :" + df.count())
    } catch {
      case e:Exception => {
        df = processOldRangeSnapshotTable(format, oldRangeSnpPath, conf)
      }
    }
    df
  }

  def processOldRangeSnapshotTable(format:String, rangeSnpPath:String, conf:SparkConf): DataFrame = {
    val spark = SparkSessionSingleton.getInstance(conf)
    var df:DataFrame = null
    try {
      df = loadFileTable(format, conf, rangeSnpPath, "range_snp")
      System.out.println(LocalDateTime.now() + " Range old snapshot read with row count :" + df.count())
    } catch {
      case e:Exception => {
        System.err.println("Encountered error reading range_snp file: " + e)
        df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], range_snp_schema)
        df.createOrReplaceTempView("range_snp")
      }
    }
    df
  }

  def processInput(input_stream_df:DataFrame, active_requirements_tbl:DataFrame, conf:SparkConf): DataFrame = {
    input_stream_df.createOrReplaceTempView("input_df")

    val spark = SparkSessionSingleton.getInstance(conf)
    val fix_input_name_df = spark.sql("select tag_name, tag_value, unit as unit_input, date_request, date_format(date_response, 'yyyy-MM-dd HH:mm:ss') as date_response, status, refinery_id as refinery_id_input from input_df")
      .withColumn("timestamp", lit(current_timestamp()))//.withWatermark("timestamp", STREAM_WATERMARK + " seconds")
    fix_input_name_df.createOrReplaceTempView("fix_input_name_df")

    //join metadata and input stream
    val input_join_df = active_requirements_tbl.join(fix_input_name_df, fix_input_name_df.col("tag_name") === active_requirements_tbl.col("id_metrics")
      && fix_input_name_df.col("refinery_id_input") === active_requirements_tbl.col("refinery_id"), "left_outer")
      .withColumnRenamed("tag_value", "unconverted_tag_value").drop("tag_value").drop("tag_name").withColumnRenamed("id_metrics", "tag_name")
    makeWindow(spark, input_join_df, Array("tag_name", "refinery_id_input","workmode","date_request"), Array(input_join_df.col("date_request").desc))
      .createOrReplaceTempView("input_join_df_unconverted")
    //input_join_df.createOrReplaceTempView("input_join_df_unconverted")

    val input_join_df_ = spark.sql(
      "select " +
        "case when ratio_formula == 0 then unconverted_tag_value * ratio " +
        "else unconverted_tag_value " +
        "end as tag_value, " +
        "* from input_join_df_unconverted")
      .withColumnRenamed("tag_value", "tag_value_r").drop("tag_value").drop("unconverted_tag_value")
    input_join_df_.createOrReplaceTempView("input_join_df_")

    spark.sql("select tag_name as r1_tag_name, tag_value_r as r1_tag_value, refinery_id as r1_refinery_id, workmode as r1_workmode, date_request as r1_date_request  from input_join_df_").createOrReplaceTempView("input_join_df_ratio1")
    val input_join_df_r2 = spark.sql("select " +
      "case when ratio_formula == 1 then abs(735.56 * tag_value_r - r1_tag_value) else tag_value_r end as tag_value, " +
      "* from (select * from input_join_df_ r0 left join input_join_df_ratio1 r1 on " +
      "r0.ratio_formula_pi_id_1 = r1.r1_tag_name " +
      "AND r0.refinery_id = r1.r1_refinery_id " +
      "AND (r0.workmode=r1.r1_workmode or (r0.workmode is null and r1.r1_workmode is null)) " +
      "AND r0.date_request = r1.r1_date_request) r")
      .drop("tag_value_r")
    input_join_df_r2.createOrReplaceTempView("input_join_df")

    input_join_df_r2
  }

  def processJoins(conf:SparkConf): DataFrame = {
    val spark = SparkSessionSingleton.getInstance(conf)
    import spark.implicits._

    val fid1=spark.sql("select tag_name as tag_name, tag_value as tag_value, refinery_id as refinery_id, " +
      "date_request as date_request, date_response as date_response, status as status, workmode as workmode " +
      "from input_join_df")
    fid1.createOrReplaceTempView("fid1")
//        System.out.println("fid1")
//        fid1.filter("tag_name='AR31:PIC26'").show()

    val fid1_4=spark.sql("select tag_name as tag_name, max(tag_value) as tag_value, refinery_id as refinery_id, " +
      "date_request as date_request, max(date_response) as date_response, max(status) as status " +
      "from input_join_df group by tag_name, refinery_id, date_request")
    fid1_4.createOrReplaceTempView("fid1_4")
//    System.out.println("fid1_4")
//    fid1_4.filter("tag_name in ('AVT6:FIRC2450.F','AVT6:FIRC2451.F','AVT6:FIRC2450.FAVT6:FIRC2451.F')").show(50,false)

    val requirements_f4=spark.sql("select id_metrics, refinery_id, workmode, formula, ids, count(*) over (partition by id_metrics, refinery_id, workmode) cnt " +
      "from (select id_metrics, refinery_id, workmode, formula, explode(split(formula_pi_id_1, '\\\\+')) as ids " +
            "from active_requirements_tbl " +
            "where activity == 'true' and formula == 4) r"
      )
    requirements_f4.createOrReplaceTempView("requirements_f4")


    val fid4=spark.sql(
      "select t.tag_name, t.refinery_id, sum(tag_value) as tag_value, date_request, max(date_response) as date_response, max(status) as status, t.workmode, count(*) cnt, max(r.cnt) max_cnt " +
//      "select * "+
      "from requirements_f4 r " +
        "left join (select i.tag_name, i.refinery_id, f1.tag_value, f1.date_request as date_request, f1.date_response as date_response, f1.status as status, i.workmode, i.ids " +
                    "from (select tag_name, refinery_id, workmode, formula, explode(split(formula_pi_id_1, '\\\\+')) as ids, date_request " +
                          "from input_join_df where formula == 4) i " +
                    "left join fid1_4 f1 on i.refinery_id = f1.refinery_id and i.ids = f1.tag_name) t  " +
                    "on r.id_metrics=t.tag_name and r.refinery_id=t.refinery_id and t.ids=r.ids and (t.workmode=r.workmode OR (t.workmode is null and r.workmode is null))" +
        "group by  t.refinery_id, t.tag_name, t.workmode, t.date_request "
        +        "having count(*)=max(r.cnt) "
       )
//      .groupBy("refinery_id", "tag_name","workmode").agg(
//      sum("tag_value").as("tag_value"),
//      first("date_request", true).as("date_request"),
//      first("date_response", true).as("date_response"),
//      first("status", true).as("status"))
    fid4.createOrReplaceTempView("fid4")
//        System.out.println("fid4")
//        fid4.filter("tag_name in ('AVT6:FIRC2450.FAVT6:FIRC2451.F')").show(50,false)

    val df = spark.sql("select i.*, " +
      "case when i.formula == 4 then nvl(i.date_request, f4.date_request) else nvl(i.date_request, f1.date_request) end as date_request_input, " +
      "case when i.formula == 4 then nvl(i.date_response, f4.date_response) else nvl(i.date_response, f1.date_response) end as date_response_input, " +
      "case when i.formula == 4 then nvl(i.status, f4.status) else nvl(i.status, f1.status) end as status_input, " +
      "frml1.tag_value as f1_tag_value, f1.tag_value as f2_tag_value_1, f1.tag_value as f3_tag_value_1, " +
      "f2.tag_value as f2_tag_value_2, f2.tag_value as f3_tag_value_2," +
      "f4.tag_value as f4_tag_value " +
      "from input_join_df i " +
      "left join fid1 frml1 on i.formula_pi_id_1 = frml1.tag_name and i.refinery_id = frml1.refinery_id and i.date_request = frml1.date_request and i.formula = 1 " +
      "left join fid1 f1 on i.formula_pi_id_1 = f1.tag_name and i.refinery_id = f1.refinery_id and i.formula != 4 and i.formula != 1 " +
      "and (i.workmode=f1.workmode or (i.workmode is null and f1.workmode is null)) " +
      "left join fid1 f2 on i.formula_pi_id_2 = f2.tag_name and i.refinery_id = f2.refinery_id and i.formula != 4 and i.formula != 1 and f1.date_request = f2.date_request " +
      "and (i.workmode=f2.workmode or (i.workmode is null and f2.workmode is null)) " +
      "left join fid4 f4 on i.tag_name = f4.tag_name and i.refinery_id = f4.refinery_id and i.formula == 4 "
      )
      .drop("date_request", "date_response", "status")
      .withColumnRenamed("date_request_input", "date_request")
      .withColumnRenamed("date_response_input", "date_response")
      .withColumnRenamed("status_input", "status")
    //df.show(50, false)

    val event_dict_tbl = Seq(
      ("normallevel", 10),
      ("criticallevel", 100),
      ("alarmlevel", 200),
      ("blockinglevel", 300),
      ("", 400),
      ("", 500),
      ("", 600)
    )
    val df_codes = df.drop("event_code").distinct().crossJoin(event_dict_tbl.toDF("event_name", "event_code").select("event_code"))
    //df_codes.createOrReplaceTempView("input_full_df_1")
    df_codes
  }

  def processConvertTopic(conf:SparkConf): DataFrame = {
    val spark = SparkSessionSingleton.getInstance(conf)
    val value_convert_df = spark.sql("select tag_name, cast(new_tag_value as varchar(100)) as tag_value, unit_input as unit, date_request, date_response, status, refinery_id from new_value_df ").distinct()
    value_convert_df.createOrReplaceTempView("value_convert_df")
    value_convert_df
  }

  def processWarningTopic(union_stream_df:DataFrame, active_requirements_tbl:DataFrame, conf:SparkConf, ct:java.time.LocalDateTime):DataFrame = {
    val spark = SparkSessionSingleton.getInstance(conf)
    import spark.implicits._
    union_stream_df.createOrReplaceTempView("union_df")

    val fix_union_name_df = spark.sql("select tag_name, tag_value, unit_input, date_request, date_format(date_response, 'yyyy-MM-dd HH:mm:ss') as date_response, " +
      "status, refinery_id as refinery_id_input, event_code, r1_tag_value, f1_tag_value, f2_tag_value_1, f2_tag_value_2, f3_tag_value_1, f3_tag_value_2, f4_tag_value, workmode as workmode_req from union_df")
    //  .withColumn("timestamp", lit(current_timestamp())).withWatermark("timestamp", STREAM_WATERMARK + " seconds")
    fix_union_name_df.createOrReplaceTempView("fix_union_name_df")

    //join metadata and input stream
    val union_join_df = fix_union_name_df.join(active_requirements_tbl, fix_union_name_df.col("tag_name") === active_requirements_tbl.col("id_metrics")
      && fix_union_name_df.col("refinery_id_input") === active_requirements_tbl.col("refinery_id")
      && (fix_union_name_df.col("workmode_req") === active_requirements_tbl.col("workmode") || (fix_union_name_df.col("workmode_req").isNull && active_requirements_tbl.col("workmode").isNull))
      ,"right_outer")
    union_join_df.createOrReplaceTempView("union_join_df")


    //unpivot
    val unpivot_df = spark.sql("select " +
      "case when event_code == 10 then norma_min " +
      "when event_code == 100 then criticallevel_min " +
      "when event_code == 200 then alarmlevel_min " +
      "when event_code == 300 then blockinglevel_min " +
      "else 'false' " +
      "end as min, " +
      "case when event_code == 10 then norma_min_value " +
      "when event_code == 100 then criticallevel_min_value " +
      "when event_code == 200 then alarmlevel_min_value " +
      "when event_code == 300 then blockinglevel_min_value " +
      "else null " +
      "end as min_value, " +
      "case when event_code == 10 then norma_max " +
      "when event_code == 100 then criticallevel_max " +
      "when event_code == 200 then alarmlevel_max " +
      "when event_code == 300 then blockinglevel_max " +
      "else 'false' " +
      "end as max, " +
      "case when event_code == 10 then norma_max_value " +
      "when event_code == 100 then criticallevel_max_value " +
      "when event_code == 200 then alarmlevel_max_value " +
      "when event_code == 300 then blockinglevel_max_value " +
      "else null " +
      "end as max_value, " +
      "* from union_join_df")
    //  .withColumnRenamed("tag_value", "unconverted_tag_value").drop("tag_value")
    unpivot_df.createOrReplaceTempView("unpivot_df")

    //calculate new value by formulas
    //ADD NEW FORMULAS HERE
    // first convert for ratio formula 1
    val new_ratio_df = spark.sql("select cast(tag_value as decimal(18, 8)) as tag_value_r, * from unpivot_df")
    new_ratio_df/*.persist(StorageLevel.MEMORY_ONLY)*/.createOrReplaceTempView("new_ratio_df")


    // second calc formulas
    val new_value_df = spark.sql(
      "select " +
        "case when formula == 0 then tag_value_r " +
        "when formula == 1 then tag_value_r " +
        "when formula == 2 then f2_tag_value_1 / f2_tag_value_2 * ratio " +
        "when formula == 3 then f3_tag_value_1 - f3_tag_value_2 * ratio " +
        "when formula == 4 then f4_tag_value " +
        "end as new_tag_value, " +
        "case when formula == 0 then min_value " +
        "when formula == 1 then f1_tag_value * norma_min_value / 100 " +
        "when formula == 2 then min_value " +
        "when formula == 3 then min_value " +
        "when formula == 4 then min_value " +
        "end as new_min_value, " +
        "case when formula == 0 then max_value " +
        "when formula == 1 then f1_tag_value * norma_max_value / 100 " +
        "when formula == 2 then max_value " +
        "when formula == 3 then max_value " +
        "when formula == 4 then max_value " +
        "end as new_max_value, " +
        "* from new_ratio_df")
    new_value_df/*.persist(StorageLevel.MEMORY_ONLY)*/.createOrReplaceTempView("new_value_df")

    //spark.sql("select * from new_value_df where formula <> 0").show(50, false)
    //new_value_df.writeStream.queryName("new_value_df").format("console").start()

    val min_max_df = spark.sql("select " +
      "case when min == 'true' then round((new_min_value - new_tag_value)*pow(10,accuracy))/pow(10,accuracy) else 0 " +
      "end as min_difference, " +
      "case when max == 'true' then round((new_tag_value - new_max_value)*pow(10,accuracy))/pow(10,accuracy) else 0 " +
      "end as max_difference, " +
      "* from new_value_df")
    min_max_df.createOrReplaceTempView("min_max_df")

    //min_max_df.writeStream.queryName("min_max_df").format("console").start()

    val difference_df = spark.sql("select " +
      "case when (status <> 0 AND event_code == 400) then 1 " +
      "when min_difference > 0 then min_difference " +
      "when max_difference > 0 then max_difference " +
      "else 0 " +
      "end as difference" +
      ", * from min_max_df")
    difference_df.createOrReplaceTempView("difference_df")

    //difference_df.writeStream.queryName("difference_df").format("console").start()

    val is_error_df = spark.sql("select " +
      "case " +
      "when (status == 0 AND difference > 0) then 1 " +
      "when (event_code == 400 AND status is not null AND status <> 0) then 1 " +
      "when (event_code == 500 AND new_tag_value is not null AND nvl(lower(unit_input),1)!=nvl(lower(unit),1)) then 1 " +
      "when (event_code == 600 AND (new_tag_value is null OR status is null)) then 1 " +
      "else 0 end as is_error " +
      ", * from difference_df")
    is_error_df.createOrReplaceTempView("is_error_df")
    //    System.out.println("is_error_df")
    //    is_error_df.filter("tag_name='AVT10:TI909'").filter("event_code=10").show()

    val snp_warn_df = spark.sql("select i.*, r.date_start, r.top_difference, r.top_value " +
      ", case when (i.date_request > r.date_start) and i.event_code!=600 then true else false end," +
      "case when r.date_start is null then true else false end, " +
      "case when i.event_code=600 then true else false end, " +
      "case when ((i.date_request > r.date_start) and i.event_code!=600) OR i.event_code=600 OR r.date_start is null then true else false end " +
      "from is_error_df i " +
      "left join range_snp r " +
      "on i.tag_name = r.tag_name and i.event_code = r.event_code and i.refinery_id = r.refinery_id and (i.workmode=r.workmode or (i.workmode is null and r.workmode is null)) "
      +      "where ((i.date_request > r.date_start) and i.event_code!=600) OR i.event_code=600 OR r.date_start is null"
    )
    snp_warn_df/*.persist(StorageLevel.MEMORY_ONLY)*/.createOrReplaceTempView("snp_warn_df")
//    System.out.println("snp_warn_df")
//    snp_warn_df.filter("tag_name in ('AVT6:FIRC2450.FAVT6:FIRC2451.F')").show(50,false)
//    snp_warn_df.show(100000,false)
//    val ct2 = ct.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
//    snp_warn_df.write.format("com.databricks.spark.csv").save(DEBUG_DF_PATH+"/snp_warn_df" + ct.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")))
//  для дебага, записываем df в файл
    toFile(snp_warn_df,"com.databricks.spark.csv",DEBUG_DF_PATH+"/snp_warn_df" + ct.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")))

    val warning_df = spark.sql("select tag_name, " +
      "case when date_start is null then date_request else date_start end as date, " +
      "new_tag_value as metrics_value, event_code, refinery_id, difference, " +
      "0 as flag from snp_warn_df")

    warning_df
  }

  def processRangeTopic(conf:SparkConf, ct:java.time.LocalDateTime): DataFrame = {
    val spark = SparkSessionSingleton.getInstance(conf)
    val ct1 = ct.plusHours(3).format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val ct2 = ct.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val new_range_snapshot_df = spark.sql(
      "select concat(tag_name, '_', refinery_id, '_', event_code) as key, refinery_id, tag_name, date_start, new_top_difference as top_difference, event_code, difference, workmode, date_request, " +
             "round((case when new_tag_value is null AND date_start is not null then top_value " +
                  "when is_error = 1 AND gr=0 AND top_difference > cast((max(difference) " +
                    "over (partition by tag_name, refinery_id, event_code, workmode, gr order by date_request rows between unbounded preceding and current row)) as DECIMAL(18,5)) " +
                    "then top_value " +
                  "when is_error = 0 AND date_end is null then null " +
                  "else first_value(new_tag_value) over (partition by tag_name, refinery_id, event_code, workmode, gr order by difference desc rows between unbounded preceding and current row) " +
            "end)*pow(10,accuracy))/pow(10,accuracy) as top_value " +
      "from (select is_error, refinery_id, tag_name, date_request, new_tag_value, top_difference, top_value,  " +
                   "case " +
                        "when new_tag_value is null AND date_start is not null then date_start " +
                        "when is_error = 1 AND gr = 0 then nvl(date_start, nvl(first_value(date_request) over (partition by tag_name, refinery_id, event_code, workmode, gr order by date_request), " +
                            "(case when refinery_id = 1 then nvl(date_request, \"" + ct1 + "\") else nvl(date_request, \"" + ct2 + "\") end)) " +
                            ") " +
                        "when is_error = 1 AND gr > 0 then nvl(first_value(date_request) over (partition by tag_name, refinery_id, event_code, workmode, gr order by date_request), " +
                            "(case when refinery_id = 1 then nvl(date_request, \"" + ct1 + "\") else nvl(date_request, \"" + ct2 + "\") end)) " +
                        "when is_error = 0 AND new_tag_value is not null then null " +
                   "else null end as date_start, " +
                   "case when is_error = 0 AND (lag(is_error) over (partition by tag_name, refinery_id, event_code, workmode order by date_request)=1 OR (gr=1 and date_start is not null)) then date_request " +
                        "else null end as date_end, " +
                   "case when new_tag_value is null AND date_start is not null then top_difference " +
                        "when is_error = 0 then null " +
                        "when is_error = 1 AND top_difference > cast((max(difference) " +
                           "over (partition by tag_name, refinery_id, event_code, workmode, gr order by date_request rows between unbounded preceding and current row)) as DECIMAL(18,5)) " +
                           "then top_difference " +
                   "else max(difference) over (partition by tag_name, refinery_id, event_code, workmode, gr) end as new_top_difference, " +
                   "event_code, difference, workmode, gr, accuracy " +
      "from (SELECT *,sum(case when nvl(lag(is_error) over (partition by tag_name, refinery_id, event_code, workmode order by date_request),is_error)=1 then 0 else 1 end) over (partition by tag_name, refinery_id, event_code, workmode order by tag_name, refinery_id, event_code, workmode, date_request) as gr FROM snp_warn_df) q) t "
    )

    val new_range_snapshot_df_gr =makeWindow(spark, new_range_snapshot_df,
      Array("tag_name", "refinery_id", "event_code", "workmode"),
//      Array(new_range_snapshot_df.col("date_start").asc))
          Array(new_range_snapshot_df.col("date_request").desc)) //для истории
    //    new_range_snapshot_df_gr.createOrReplaceTempView("new_range_snapshot_df_gr")

    new_range_snapshot_df_gr.createOrReplaceTempView("new_range_snapshot_df_gr")
    val old_snp=spark.sql("select snp.* " +
      "from range_snp snp left join new_range_snapshot_df_gr n " +
      "on snp.tag_name = n.tag_name " +
      "   and snp.event_code = n.event_code " +
      "   and snp.refinery_id = n.refinery_id " +
      "   and (snp.workmode=n.workmode or (snp.workmode is null and n.workmode is null)) " +
      " where n.tag_name is null")
    System.out.println("old_snp count:" + old_snp.count())

//    System.out.println("old_snp:")
//    old_snp.printSchema()
//    System.out.println("new_range_snapshot_df_gr:")
//    new_range_snapshot_df_gr.printSchema()
//        System.out.println("new_range_snapshot_df_gr")
//        new_range_snapshot_df_gr.filter("tag_name in ('2:KT-1:FCA1201-1')").show()
//        new_range_snapshot_df_gr.show(20000,false)
//    new_range_snapshot_df_gr.write.format("com.databricks.spark.csv").save(DEBUG_DF_PATH+"/new_range_snapshot_df_gr" + ct.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")))
//  для дебага, записываем df в файл
    toFile(new_range_snapshot_df_gr,"com.databricks.spark.csv",DEBUG_DF_PATH+"/new_range_snapshot_df_gr" + ct.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")))
    new_range_snapshot_df_gr.union(old_snp)
  }

  def processRangeOutput(conf:SparkConf, ct:java.time.LocalDateTime): DataFrame = {
    val spark = SparkSessionSingleton.getInstance(conf)
    val ct1 = ct.plusHours(3).format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val ct2 = ct.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val range_output_df = spark.sql(
        "select id_metrics, refinery_id, event_code, workmode, date_start, " +
          "case when (event_code = 600 AND date_end is not null) then case when refinery_id = 1 then  \"" + ct1 + "\" else \"" + ct2 + "\" end " +
          "else date_end end as date_end, " +
          "round(metrics_value*pow(10,accuracy))/pow(10,accuracy) as metrics_value, difference, top_difference, new_top_difference, nvl(top_difference,0) nvl_diff, " +
          "case when nvl(top_difference,0)<new_top_difference then 'true' else 'false' end as check, gr, old_date_start, is_error  " +
        "from ( " +
          "select refinery_id, id_metrics, date_start, date_end, top_difference, event_code, difference, new_top_difference, workmode, gr, old_date_start, is_error, " +
            "accuracy, " +
            "lag(is_error) over (partition by id_metrics, refinery_id, event_code, workmode order by date_request) as lag_err, " +
            "case when is_error = 0 AND date_end is null then null " +
                 "when ((is_error = 1 AND gr=0) OR (is_error = 0 AND gr=1)) AND top_difference > cast((max(difference) " +
                   "over (partition by id_metrics, refinery_id, event_code, workmode, gr order by date_request rows between unbounded preceding and current row)) as DECIMAL(18,5)) " +
                   "then top_value " +
            "else max(new_tag_value) over (partition by id_metrics, refinery_id, event_code, workmode, gr order by date_request rows between unbounded preceding and current row) end as metrics_value " +
          "from ( " +
            "select is_error, refinery_id, id_metrics, date_request, new_tag_value, top_difference, top_value, " +
              "accuracy, " +
              "case when gr = 0 then nvl(date_start, nvl(first_value(date_request) over (partition by tag_name, refinery_id, event_code, workmode, gr order by date_request), " +
                                                                        "(case when refinery_id = 1 then nvl(date_request, \"" + ct1 + "\") else nvl(date_request, \"" + ct2 + "\") end)) " +
                                                                        ") " +
                   "when is_error = 1 AND gr > 0 then nvl(first_value(date_request) over (partition by tag_name, refinery_id, event_code, workmode, gr order by date_request), " +
                                                        "(case when refinery_id = 1 then nvl(date_request, \"" + ct1 + "\") else nvl(date_request, \"" + ct2 + "\") end)) " +
                   "when is_error = 0 AND gr = 1 AND date_start is not null then date_start " +
//                   "when is_error = 0 AND gr = 0 AND date_start is not null then date_start " +
                   "when is_error = 0 AND lag(is_error) over (partition by tag_name, refinery_id, event_code, workmode, gr order by date_request)!=is_error then first_value(date_request) over (partition by tag_name, refinery_id, event_code, workmode, gr order by date_request) " +
              "else null end as date_start, " +
            "case when is_error = 0 AND (lag(is_error) over (partition by tag_name, refinery_id, event_code, workmode order by date_request)=1 OR (gr=1 and date_start is not null)) then date_request " +
            "else null end as date_end, " +
            "case when is_error = 0 then null " +
                 "when is_error = 1 AND top_difference > cast((max(difference) " +
                   "over (partition by tag_name, refinery_id, event_code, workmode, gr order by date_request rows between unbounded preceding and current row)) as DECIMAL(18,5)) " +
                   "then top_difference " +
                 "else max(difference) over (partition by tag_name, refinery_id, event_code, workmode, gr) end as new_top_difference, " +
            "event_code, difference, workmode, gr, date_start as old_date_start " +
            "from (SELECT *,sum(case when nvl(lag(is_error) over (partition by tag_name, refinery_id, event_code, workmode order by date_request),is_error)=1 then 0 else 1 end) over (partition by tag_name, refinery_id, event_code, workmode order by tag_name, refinery_id, event_code, workmode, date_request) as gr FROM snp_warn_df) q) t " +
            "where t.is_error == 1 or date_end is not null " +
          ") nf "
          +
          "where (cast(nvl(top_difference,0) as  DECIMAL(18,5)) < cast(new_top_difference as DECIMAL(18,5)) AND gr=0) " +
            "OR lag_err=0 " +
            "OR date_end is not null " +
            "OR (event_code in (400, 500, 600) AND old_date_start is null)"
    )

//    range_output_df.createOrReplaceTempView("range_output_df")
//    System.out.println("range_output_df")
//    range_output_df.filter("id_metrics in ('2:KT-1:FCA1201-1')").show()
//    range_output_df.show(2000,false)
//    range_output_df.write.format("com.databricks.spark.csv").save(DEBUG_DF_PATH+"/range_output_df" + ct.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")))
//  для дебага, записываем df в файл
    toFile(range_output_df,"com.databricks.spark.csv",DEBUG_DF_PATH+"/range_output_df" + ct.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")))
    range_output_df
  }

  def getDStream(topic:String, brokers:String, ssc:StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> STREAM_GROUP,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](Array(topic), kafkaParams))
  }

  def convertFromJson(rddRaw:RDD[ConsumerRecord[String, String]], schema: StructType): DataFrame = {
    SparkSessionSingleton.getInstance(rddRaw.context.getConf).read.schema(schema).json(rddRaw.map(_.value()))
  }

  def convertStreamToDf(stream:InputDStream[ConsumerRecord[String, String]], name:String, schema:StructType, spark:SparkSession): Unit = {
    stream.foreachRDD(rddRaw => {
      val df = convertFromJson(rddRaw, schema)
      df.createOrReplaceTempView(name)
    })
  }

  def toKafka(brokers:String, topic:String, cpDir:String, df:DataFrame): DataFrameWriter[Row] = {
    df.selectExpr("CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("checkpointLocation", cpDir + topic)
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", topic)
  }

  def toFile(df:DataFrame, format:String, path:String, saveMode:SaveMode = SaveMode.Overwrite): Unit = {
    df.write.format(format).mode(saveMode).save(path)
  }

  def toHBase(df:DataFrame, cat: String, options: Map[String, String] = Map.empty): Unit = {
    val c = df.count()
    if(c > 0) {
      df.write
        .options(Map(HBaseTableCatalog.newTable -> "1", HBaseTableCatalog.tableCatalog -> cat) ++ options)
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .save()
    }
  }

  def fromHBase(spark:SparkSession, viewName:String, cat: String, options: Map[String, String] = Map.empty, emptyToNull:Seq[String] = Seq.empty): DataFrame = {
    var df = spark.read
      .options(Map(HBaseTableCatalog.tableCatalog->cat) ++ options)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    if(emptyToNull.nonEmpty) {
      emptyToNull.foreach(c => df = df.withColumn(c, when(col(c).equalTo(""), null)))
    }
    df.createOrReplaceTempView(viewName)
    df
  }

  def makeWindow(sql: SparkSession, df: DataFrame, arrCol: Array[String], sortCols: Array[Column]): DataFrame = {
    import sql.implicits._
    val w = Window.partitionBy(arrCol.head, arrCol.tail: _*).orderBy(sortCols: _*)
    val dfTop = df.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")
    dfTop
  }

  /** Case class for converting RDD to DataFrame */
  case class Record(word: String)


  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}
