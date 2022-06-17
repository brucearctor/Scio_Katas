import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.GlobalWindow
import org.apache.beam.sdk.values.PCollection
import org.slf4j.Logger
import org.slf4j.LoggerFactory


object Log {
  private val LOG = LoggerFactory.getLogger(classOf[Log])

  def ofElements[T] = new Log.LoggingTransform[T]

  def ofElements[T](prefix: String) = new Log.LoggingTransform[T](prefix)

  private class LoggingTransform[T] private() extends PTransform[PCollection[T], PCollection[T]] {
    prefix = ""
    private var prefix = null

    def this(prefix: String) {
      this()
      this.prefix = prefix
    }

    override def expand(input: PCollection[T]): PCollection[T] = input.apply(ParDo.of(new DoFn[T, T]() {
      @ProcessElement def processElement(@Element element: T, out: DoFn.OutputReceiver[T], window: BoundedWindow): Unit = {
        var message = prefix + element.toString
        if (!window.isInstanceOf[GlobalWindow]) message = message + "  Window:" + window.toString
        LOG.info(message)
        out.output(element)
      }
    }))
  }
}

class Log private() {
}