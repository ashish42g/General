package escrowproxy

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object EscrowProxyDF {

  val ICDP: String = "ICDP."
  val OS: String = "OS."
  val UU: String = ".UU"
  val DVC: String = "DVC."
  val SUCC_RES_CODE: String = "200"

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if (args.length < 2) {
      println(("This job expects two command line arguments: args[0] is input file path, args[2] is output file path"))
      System.exit(-1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("EscrowProxyDF")
    val sc = new SparkContext(conf)
    val fileData = sc.textFile(args(0))

    val utility: Utility = new Utility()
    val entities = fileData.map(utility.parse)

    val KPIMap = scala.collection.mutable.HashMap[String, Int]()

    val filteredEntities = entities.filter(entity => entity.isValid && entity.command.matches("RECOVER"))

    filteredEntities.persist()

    val filteredOSEntities = filteredEntities.filter(entity => entity.os_valid)

    val OS_U = filteredOSEntities.map(entity => (ICDP + entity.DF_NAME_SUFFIX + OS + entity.os_type + UU, entity.prs_id))
    val OS_UU = OS_U.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //OS_UU.collect.foreach(println)

    val OS_MAJ_VER = filteredOSEntities.map(entity => (ICDP + entity.DF_NAME_SUFFIX + OS + entity.os_type + "." + entity.os_major_version + UU, entity.prs_id))
    val OS_MAJ_VER_UU = OS_MAJ_VER.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //OS_MAJ_VER_UU.foreach(println)

    val OS_MAJ_MIN = filteredOSEntities.map(entity => (ICDP + entity.DF_NAME_SUFFIX + OS + entity.os_type + "." + entity.os_major_version + entity.os_minor_version + UU, entity.prs_id))
    val OS_MAJ_MIN_UU = OS_MAJ_MIN.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //OS_MAJ_MIN_UU.foreach(println)


    val filteredDeviceEntities = filteredEntities.filter(entity => entity.device_valid)

    val DEVICE_TYPE = filteredDeviceEntities.map(entity => (ICDP + entity.DF_NAME_SUFFIX + DVC + entity.platform_type + UU, entity.prs_id))
    val DEVICE_TYPE_UU = DEVICE_TYPE.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //DEVICE_TYPE_UU.foreach(println)

    val DEVICE_NAME = filteredDeviceEntities.filter(entity => entity.platform.contains("MAC")).map(entity => (ICDP + entity.DF_NAME_SUFFIX + DVC + entity.platform_name + UU, entity.prs_id))
    val DEVICE_NAME_UU = DEVICE_NAME.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //DEVICE_NAME_UU.foreach(println)

    val filteredMacDevEntities = filteredDeviceEntities.filter(entity => !entity.platform.contains("MAC"))
    val MAC_VER = filteredMacDevEntities.map(entity => (ICDP + entity.DF_NAME_SUFFIX + DVC + entity.platform_name + "." + entity.platform_version + UU, entity.prs_id))
    val MAC_VER_UU = MAC_VER.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //MAC_VER_UU.foreach(println)


    val filCommandEntities = filteredEntities.filter(entity => entity.command != null)

    val RECOVER = filCommandEntities.map(entity => (ICDP + "RECOVER" + UU, entity.prs_id))
    val RECOVER_UU = RECOVER.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //RECOVER_UU.foreach(println)

    val RECOVER_CNT = filCommandEntities.map(entity => (ICDP + "RECOVER.CNT", entity.aggrCt)).aggregateByKey(0)((a, v) => a + v.toInt, _ + _)
    //RECOVER_CNT.foreach(println)

    val filtSuccessEntities = filCommandEntities.filter(entity => entity.response != null && entity.response.contains(SUCC_RES_CODE))

    val RECOVER_SUCCESS = filtSuccessEntities.map(entity => (ICDP + "RECOVER.SUCCESS" + UU, entity.prs_id))
    val RECOVER_SUCCESS_UU = RECOVER_SUCCESS.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //RECOVER_SUCCESS_UU.foreach(println)

    val RECOVER_SUCCESS_CNT = filtSuccessEntities.map(entity => (ICDP + "RECOVER.SUCCESS.CNT", entity.aggrCt)).aggregateByKey(0)((a, v) => a + v.toInt, _ + _)
    //RECOVER_SUCCESS_CNT.foreach(println)


    val filtFailEntities = filCommandEntities.filter(entity => entity.response != null && !entity.response.contains(SUCC_RES_CODE))

    val RECOVER_FAIL = filtFailEntities.map(entity => (ICDP + "RECOVER.FAILURE" + UU, entity.prs_id))
    val RECOVER_FAIL_UU = RECOVER_FAIL.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //RECOVER_FAIL_UU.foreach(println)

    val RECOVER_FAIL_CNT = filtFailEntities.map(entity => (ICDP + "RECOVER.FAILURE.CNT", entity.aggrCt)).aggregateByKey(0)((a, v) => a + v.toInt, _ + _)
    //RECOVER_FAIL_CNT.foreach(println)

    val REC_FAIL_SUCC_UU = (ICDP + "RECOVER.FAILSUCCESS" + UU, RECOVER_SUCCESS.map(l => (l._2, "")).join(RECOVER_FAIL.map(l => (l._2, ""))).count.toInt)
    //println(REC_FAIL_SUCC_UU)

    val filtRecordEntities = filCommandEntities.filter(entity => entity.label.contains("record"))

    val RECORD = filtRecordEntities.map(entity => (ICDP + "RECOVER.RECORD.CNT", entity.prs_id))
    val RECORD_CNT = RECORD.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //RECORD_CNT.foreach(println)

    val filtRecordSucEnt = filtRecordEntities.filter(entity => entity.response != null && entity.response.contains(SUCC_RES_CODE))

    val RECORD_SUCCESS = filtRecordSucEnt.map(entity => (ICDP + "RECOVER.RECORD.SUCCESS" + UU, entity.prs_id))
    val RECORD_SUCCESS_UU = RECORD_SUCCESS.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //RECORD_SUCCESS_UU.foreach(println)

    val filtRecordFailEnt = filtRecordEntities.filter(entity => entity.response != null && !entity.response.contains(SUCC_RES_CODE))

    val RECORD_FAIL = filtRecordFailEnt.map(entity => (ICDP + "RECOVER.RECORD.FAILURE" + UU, entity.prs_id))
    val RECORD_FAIL_UU = RECORD_FAIL.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //RECORD_FAIL_UU.foreach(println)


    val filtPCFailEntities = filtFailEntities.filter(entity => entity.errorCd.contains("-6015"))

    val PC_FAIL = filtPCFailEntities.map(entity => (ICDP + "RECOVER.PCFAILURE" + UU, entity.prs_id))

    val PC_FAIL_UU = PC_FAIL.distinct.aggregateByKey(0)((a, v) => a + 1, _ + _)
    //PC_FAIL_UU.foreach(println)

    val PC_FAIL_CNT = filtPCFailEntities.map(entity => (ICDP + "RECOVER.PCFAILURE.CNT", entity.aggrCt)).aggregateByKey(0)((a, v) => a + v.toInt, _ + _)
    //PC_FAIL_CNT.foreach(println)
  }
}
