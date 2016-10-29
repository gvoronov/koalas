package koalas.csvfile

import java.io.{FileReader, InputStreamReader, BufferedReader}

object CSVFile{
  // def read(filePath: String, delimiter: String = ",", header: Boolean = true): List[List[String]] = {
  def read(
      filePath: String, delimiter: String = ",", header: Boolean = true,
      readerType: String = "File"): List[Map[String, String]] = {
    val reader = readerType match {
      case "File" => {
        val file = new FileReader(filePath)
        new BufferedReader(file)
      }
      case "InputStream" => {
        val inputStream = new InputStreamReader(getClass().getResourceAsStream(filePath))
        new BufferedReader(inputStream)
      }
    }

    var data: List[List[String]] = List.empty
    // var row: List[String] = List()

    try {
      var line: String = null
      while ({line = reader.readLine(); line} != null) {
        // Eventually fix this to allow delimiters inside string objects
        val row: List[String] = line.split(delimiter).map(_.trim).toList
        data = data :+ row
      }
    } finally {
      reader.close()
    }

    type HeadTailSplit = Tuple2[List[String], List[List[String]]]
    val (columns, dataTail): HeadTailSplit = if (header) {
      (data.head, data.tail)
    } else {
      val columns = List.range(0, data.head.length).map(_.toString)
      (columns, data)
    }
    dataTail.map(row => columns.zip(row).toMap)
  }
  def write(path: String): Unit = {}
}
