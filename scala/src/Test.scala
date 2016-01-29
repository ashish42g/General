
object Test {
  def main(args: Array[String]) {
    val source = scala.io.Source.fromFile("/Users/ashish/IdeaProjects/General/data/textData/users.txt")
    val lines = try source.mkString finally source.close()
    val mapOp = lines.map(_.toString.split('\t'))

  }
}
