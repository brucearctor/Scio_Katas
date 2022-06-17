package org.apache.beam.learning.katas.scala.intro.map

import com.spotify.scio._
// TODO: CREATE FILE TO READ FROM, sc.parrallelize, sc.wrap, OR BEAM.CREATE...
// TODO: UPPERCASE or MULTIPLY BY TEN?


class Task {

  object WordCount {

    val data = Seq(1,2,3,4,5)

    def extractFlowInfo(input: SCollection[String]): SCollection[(String, Int)] =
      input

    //val inData = Seq("a b c d e", "a b a b", "")
    val expected = Seq(10,20,30,40,50)
    def main(cmdlineArgs: Array[String]): Unit = {
      // Parse command line arguments, create `ScioContext` and `Args`.
      // `ScioContext` is the entry point to a Scio pipeline. User arguments, e.g.
      // `--input=gs://[BUCKET]/[PATH]/input.txt`, are accessed via `Args`.
      val sc = ScioContext()

        //sc.wrap(Pipeline.create().apply(Create.of("Hello World").withCoder(StringUtf8Coder.of())))
        sc.parallelize(data)
                 .transform("by10") {
                   _.map(e => e * 10)
                 }
          .saveAsTextFile("/tmp/sciooutput.txt")

      sc.run()



    }
  }


}