package gakesson.util.concurrent.scala

import org.fest.assertions.Assertions.assertThat
import org.fest.assertions.Assertions
import java.util.concurrent.atomic.AtomicInteger
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import scala.annotation.tailrec
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class NonBlockingStackTest extends Assertions {

  @BeforeMethod
  def setupTestCase() {
    Element.instanceCounter.set(0)
  }

  @Test
  def shouldPushElement() {
    val stack = new NonBlockingStack[Element]

    val element = new Element()
    stack.push(element)

    assertThat(stack.peek()).isSameAs(element)
  }

  @Test
  def shouldPushAndPopElementsInLIFO() {
    val numberOfElements = 100
    val stack = new NonBlockingStack[Element]
    val elements = createAndPushElements(numberOfElements, stack)
    val reverseIndex = elements.length - 1

    @tailrec def assertOne(index: Int = reverseIndex) {
      if (index >= 0) {
        val element = stack.pop()
        assertThat(element).isSameAs(elements(index))
        assertOne(index - 1)
      }
    }
    assertOne()
  }

  @Test
  def shouldReturnCorrectSizeOfStack() {
    val numberOfElements = 100
    val stack = new NonBlockingStack[Element]
    createAndPushElements(numberOfElements, stack)

    assertThat(stack.size()).isEqualTo(numberOfElements)
  }

  @Test
  def shouldReturnZeroSizeWhenStackIsEmpty() {
    val stack = new NonBlockingStack[Element]

    assertThat(stack.size()).isZero()
  }

  @Test
  def shouldReturnIsEmptyWhenStackHasNoElements() {
    val stack = new NonBlockingStack[Element]

    assertThat(stack.isEmpty()).isTrue()
  }

  @Test
  def shouldNotReturnIsEmptyWhenStackHasElements() {
    val numberOfElements = 100
    val stack = new NonBlockingStack[Element]
    createAndPushElements(numberOfElements, stack)

    assertThat(stack.isEmpty()).isFalse()
  }

  @Test
  def concurrentShouldPushAndPopElements() {
    val numberOfElements = 10000
    val stack = new NonBlockingStack[Element]
    val executor = Executors.newCachedThreadPool()
    val parallelism = 20
    val resultBarrier = new CountDownLatch(parallelism)
    val wasOK = new AtomicBoolean(true)

    @tailrec def spinOffOneThread(count: Int = 0) {
      if (count < parallelism) {
        val op = new Runnable() {
          def run() {
            try {
              @tailrec def executeOperationFor(count: Int = 0, op: Unit => Unit) {
                if (count < numberOfElements) {
                  op()
                  executeOperationFor(count + 1, op)
                }
              }
              executeOperationFor(op = (_ => stack.push(new Element())))
              executeOperationFor(op = (_ => assertThat(stack.pop()).isNotNull()))
            } catch {
              case t: Throwable => {
                wasOK.set(false)
                throw new RuntimeException(t)
              }
            } finally {
              resultBarrier.countDown()
            }
          }
        }
        executor.execute(op)
        spinOffOneThread(count + 1)
      }
    }

    spinOffOneThread()
    val ok = resultBarrier.await(5, TimeUnit.SECONDS)

    assertThat(ok).isTrue()
    assertThat(stack.isEmpty()).isTrue()
    assertThat(wasOK.get()).isTrue()

    executor.shutdownNow()
  }

  private def createAndPushElements(numberOfElements: Int, stack: Stack[Element]): List[Element] = {
    @tailrec def createOne(elements: List[Element] = Nil, count: Int = 0): List[Element] = {
      if (count < numberOfElements) {
        createOne(new Element() :: elements, count + 1)
      } else elements
    }
    val createdElements = createOne().reverse
    createdElements.foreach(stack.push(_))
    createdElements
  }

  private class Element(val instanceId: Int = Element.instanceCounter.incrementAndGet())
  private object Element { val instanceCounter = new AtomicInteger(0) }
}