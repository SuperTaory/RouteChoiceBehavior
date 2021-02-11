
object exercise {
  def main(args: Array[String]): Unit = {

    val s = "2019-06-30 00:59:05"
    val l = CommonFunctions.transTimeToTimestamp(s)
    println(l)
    println(CommonFunctions.hourOfDay(s))
//    println(monthOfYear(l))
//    println(dayOfWeek(CommonFunctions.transTimeToTimestamp(s)))
//    println(dayOfMonth_long(l))
//    println(quarterOfDay(l))
//    print(CommonFunctions.hourOfDay(s))
  }
}
