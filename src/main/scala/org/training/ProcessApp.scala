


package org.training

import org.joda.time.DateTime

/** @author Pooja Nair**/
/*Main Entry class*/

object ProcessApp extends DemandPlanningConf {
	println("Inside main")
	println("test")

	try {
//		val args_len = args.length
		val userInput = args
		println("Total start" + DateTime.now())  // Total start" 2020-21-12 12:03:23
				rootLogger.info("------UDF register started------") // INFO : ------UDF register started------

	/*	val  udf = new UDFProcessor(sparkContext,sqlContext)
      udf.process()*/
    	rootLogger.info("------UDF registering completed------")

		rootLogger.info("------Loading DMT Processor------")
		val dmtProcessor = new Processor(userInput, sparkContext, sqlContext,sparkSession)
		dmtProcessor.process()
		rootLogger.info("------Completed DMT Processor------")
		println("Total end" + DateTime.now())

	} catch {
		case t : Throwable =>
			t.printStackTrace() // TODO: handle error
			log.error("Error in ProcessApp", t);
	} finally {
		sparkContext.stop()
		println("Spark Context Closed : " + sparkContext.isStopped)

	}

}


