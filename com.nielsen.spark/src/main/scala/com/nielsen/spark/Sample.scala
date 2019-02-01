

package com.nielsen.spark

object Sample {

  def main(args: Array[String]): Unit = {

    keyValReplace()
    //newk()
  }
  def keyValReplace() =
    {
      val a = Map("1" -> "vijay", "2" -> "bhargav", "3" -> "madhu")

      var b = Array("1", "bhargav", "3")

      for (v <- 0 to b.length - 1) {

        if (a.getOrElse(b(v), null) != "null") {
          val aq = a.getOrElse(b(v), null)
          if (aq != null)
            println(aq)
        }
      }
    }
  def newk() =
    {
      val av = Map("1" -> "vijay", "2" -> "bhargav", "3" -> "madhu")

      val bs = List("1", "bhargav", "3")
     
     //1----------> print(b flatMap { k => a get k })
      
      
      
    //2-------> print( bs flatMap (av get))
      
      
   // if same keys and list values exist then -->  print(bs map av)
      
      
      for (k <- bs; v <- av get k) yield println(v)

    }
}