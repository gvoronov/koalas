import java.io.{FileReader, BufferedReader}

object CSVFile{
  def read(filePath: String, delimiter: String = ","): List[List[String]] = {
    val file = new FileReader(filePath)
    val reader = new BufferedReader(file)

    var data: List[List[String]] = List()
    var row: List[String] = List()

    try {
      var line: String = null
      while ({line = reader.readLine(); line} != null) {
        // Eventually fix this to allow delimiters inside string objects
        row = line.split(delimiter).map(_.trim).toList
        data = data :+ row
      }
    } finally {
      reader.close()
    }

    data
  }
  def write(path: String): Unit = {}
}
