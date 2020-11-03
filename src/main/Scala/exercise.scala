import CommonFunctions.{dayOfWeek, monthOfYear, transTimeToString, quarterOfDay, dayOfMonth_long, halfHourOfMonth, transTimeToTimestamp}
object exercise {
  def main(args: Array[String]): Unit = {

    val s = "2019-06-30 23:59:05"
    val l = transTimeToTimestamp(s)
    println(l)
    println(transTimeToString(l+10))
//    println(monthOfYear(l))
//    println(dayOfWeek(CommonFunctions.transTimeToTimestamp(s)))
//    println(dayOfMonth_long(l))
//    println(quarterOfDay(l))
//    print(CommonFunctions.hourOfDay(s))
  }
}
