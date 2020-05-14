import GeneralFunctionSets.{dayOfWeek, monthOfYear, transTimeToString, quarterOfDay, dayOfMonth_long}
object exercise {
  def main(args: Array[String]): Unit = {
    val l = 1588521545L
    println(transTimeToString(l))
    println(monthOfYear(l))
    println(dayOfWeek(l))
    println(dayOfMonth_long(l))
    println(quarterOfDay(l))
  }
}
