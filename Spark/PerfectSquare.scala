object PerfectSquare {
  def main(args:Array[String])
  {
    var count:Int = 0;
    print("Enter number: ")
    var num=scala.io.StdIn.readInt()
    print("Enter bills: ")
    var str = scala.io.StdIn.readLine()
    val x : Array[String] = str.split(" ")
    val y : Array[Int] = x.map(x=>x.toInt)
    //for(i<- y) print(i)

    for(i<-0 to num-1)
    {
      for(j<-1 to scala.math.sqrt(y(i)).toInt)
      {
        if (j*j==y(i))
        {
          count=count+1
        }
      }
    }
    print(count)
  }
}
