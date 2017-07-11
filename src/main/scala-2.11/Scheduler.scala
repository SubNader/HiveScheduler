import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.{ StructType, StructField, StringType, IntegerType, DateType };
import scala.io.Source._
import sys.process._
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object Scheduler {
  def welcome_message(version: String) {
    println("#" * 50 + "\n#" + " " * 48 + "#\n# \t\tHiveScheduler (V" + version + ")\t\t #\n#" + " " * 48 + "#\n" + "#" * 50)
  }
  def main(args: Array[String]) {
    
    // Header
    welcome_message("1.0");
    
    // Configuration
    println("\nOozie configuration: ")
    val oozie_path = readLine("Oozie path = ")
    val properties_file_path = readLine("Job properties file path = ")
    var queries_directory = readLine("Queries directory = ")
    
    // Validate queries directory
    while(!scala.reflect.io.File(scala.reflect.io.Path(queries_directory)).exists)
    {
      queries_directory = readLine("Invalid directory. Queries directory = ")
    }
    
    // Fetch and display query files (if any)
    val queries = Process("ls " + queries_directory).lines
    
    if (queries.size > 0) {
      println("\n" + queries.size + " query files found:")
      queries.foreach(println)
    } else {
      println("No query files found. Exiting..")
      sys.exit(0)
    }
    println("\nType schedule to schedule a new query or exit to quit.");

    //Console
    while (true) {
      
      val user_input = readLine("\nHive Scheduler> ");

      //Handle modes
      if (user_input.compareToIgnoreCase("schedule") == 0) {

        //Load query file from directory
        var query_file_name = readLine("Query file name = ")
        while (!queries.contains(query_file_name)) {
          query_file_name = readLine("Query file not found..\nQuery file name = ")
        }
        val query = fromFile(queries_directory +"/" + query_file_name).getLines.mkString
        val regex = "\\$([\\w.$]+|\"[^\"]+\"|'[^']+')".r
        val parameters = regex.findAllIn(query).toList
        if (parameters.size > 0) {
          print("\nQuery file " + query_file_name + " has been loaded successfully. " + parameters.size)
          if (parameters.size > 1) print(" parameters found.") else print(" parameter found.")
        } else {
          println("\n" + query_file_name + " has been loaded successfully. No parameters found.")
        }
        
        //Set query parameters
        println("\n\nSet query parameters:")
        val parameters_map = parameters.map {parameter => 
        val parameter_value = readLine(parameter.subSequence(1, parameter.length) + " = ")
        (parameter,parameter_value)
        } 
        
        // Generate final command
        var final_command = "sudo oozie job -oozie " + oozie_path +
                          " -DhiveQueryFilePath=" +queries_directory +
                          "/"+query_file_name
        parameters_map.foreach(
            pair=>            
            final_command=final_command.concat(" -D"+pair._1.subSequence(1, pair._1.length) + "=" + pair._2)
        )
        final_command = final_command.concat(" -config " + properties_file_path + " -run")
        println("\nFinal command:\n" + final_command)
        if(readLine("\nSchedule job? (Y/N) = ").equalsIgnoreCase("y")){
          try{
            val execution_response = final_command!!
            
            println("Execution response:\n" + execution_response)
            
          }
          catch{
            case _: Throwable =>  
              println("Execution failed. Exiting..")
              sys.exit(1)
          }
        }
      } 
      // Termination
      else if (user_input.compareToIgnoreCase("exit") == 0) {
        println("\nExiting..")
        sys.exit(0)
      }
      else {
        println("Invalid command")
      }
    }
  }
}