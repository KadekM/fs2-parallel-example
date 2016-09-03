import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import fs2._

import scala.io.StdIn
import scala.util.Random

/**
  * This starts the "download" query process using all IO threads
  * fetching Batches... as soon as as we have enough of them (chunkSize)
  * we serialize it on some thread (while other keep downloading
  * remaining stuff)
  *
  * Play with various values
  */
object ProcessAllSerializeChunk extends App {
  val unsafeRandom = new Random()
  def unsafeNow = LocalDateTime.now.truncatedTo(ChronoUnit.SECONDS).toString

  case class Thing(id: Int)
  type BatchedThing = Seq[Thing]

  // change to see what happens if its higher than number of IO threads, what happens when its less etc
  val totalNumberOfThings = 100 // total amount of jobs you need to do
  val numberOfIOThreads = 15  // how many threads are you willing to sacrifice for this io
  val maxQueriesOpen = numberOfIOThreads // how many threads is stream allowed to use
  val chunkSize = 5 // what is the chunk size (series of BatchedThings) that you wish to serialize

  implicit val strategy: Strategy =
    Strategy.fromFixedDaemonPool(numberOfIOThreads, "IO") // setup bigger*/

  // Generate sequence of jobs
  def queries: Seq[Task[BatchedThing]] =
    (1 to totalNumberOfThings).map { _ =>
      Task.delay {
        val thingId = Thing(unsafeRandom.nextInt)
        println(
          s"- [$unsafeNow ${Thread.currentThread().getName}] starting to fetch thing with $thingId id")
        val workDuration = unsafeRandom.nextInt(3 * 1000)
        Thread.sleep(workDuration)
        println(
          s"✓ [$unsafeNow ${Thread.currentThread().getName}] fetched:$thingId, time-required: $workDuration ms")
        Seq(thingId)
      }
    }

  // Serialize batch (usually its flattened several batches)
  def serializeThings(xs: BatchedThing): Task[Unit] = Task.delay {
    println(s"-# starting serialization of $xs")
    val workDuration = unsafeRandom.nextInt(2000 + 5 * 1000) // serialization is gonna take some time
    Thread.sleep(workDuration)
    println(s"✓# serialized $xs")
  }

  // actual algorithm is here:
  val jobs = queries.map(Stream.eval) // each query is a short stream evaluating task
  val jobsStream = Stream.emits(jobs)
  val runnable =
    concurrent
      .join(maxQueriesOpen)(jobsStream)
      .scan(1L, Seq.empty[Thing]) {
        // count jobs completed and keep flattening them to one big batch
        case ((cnt, batched), newBatch) =>
          if ((cnt - 1L) % chunkSize == 0) // if we have just serialized, start new batch
            (cnt + 1L, newBatch)
          else // otherwise continue building the batch
            (cnt + 1L, Seq.concat(batched, newBatch))
      }
      .filter { case (cnt, _) => cnt % chunkSize == 0L } // we are only interested to serialize these big chunks
      .map { case (_, batchedThings) => batchedThings } // we are no longer interested in count

  runnable.evalMap(serializeThings).run.unsafeRunAsyncFuture

  println("press enter to exit")
  StdIn.readLine()
}
