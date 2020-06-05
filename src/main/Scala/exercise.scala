import CommonFunctions.{dayOfWeek, monthOfYear, transTimeToString, quarterOfDay, dayOfMonth_long}
object exercise {
  def main(args: Array[String]): Unit = {
    val l = 1588521545L
    val s = "2020-05-24 00:59:05"
    println(transTimeToString(l))
    println(monthOfYear(l))
    println(dayOfWeek(CommonFunctions.transTimeToTimestamp(s)))
    println(dayOfMonth_long(l))
    println(quarterOfDay(l))
    print(CommonFunctions.hourOfDay(s))
  }
}
