package com.zsk

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCountScala {
  def main(args: Array[String]): Unit = {
    //1. get params
    val params: ParameterTool = ParameterTool.fromArgs(args)
    //2.set env
    val env = ExecutionEnvironment.getExecutionEnvironment
    //3.set params globalable
    env.getConfig.setGlobalJobParameters(params)
    //4.add source
    val text =
      if (params.has("input")) {
        println("Executing WordCount example with default input data set.")
        println("Use --input to specify file input.")
        env.readTextFile(params.get("input"))
      }
      else {
        env.fromCollection(WordCountData.WORDS)
      }

    //5 transformation
    val counts = text.flatMap(_.toLowerCase.split("\\W+"))
      .map((_,1))
      .groupBy(0)
      .sum(1)
    //6 sink
    if(params.has("output")){
      counts.writeAsCsv(params.get("output"), "\n", " ")
      env.execute("scala WordCount example")
    }else{
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }
  }

}
