package gakesson.util.concurrent.scala

import java.util.concurrent.atomic.AtomicInteger
import gakesson.util.concurrent.scala.BoundedBlockingQueueTest2.ExecutionInjection
import org.testng.annotations.Test
import java.util.Queue
import java.util.LinkedList
import org.fest.assertions.Assertions.assertThat
import java.util.ArrayList
import java.util.concurrent.BlockingQueue
import scala.annotation.tailrec
import java.util.Collections
import org.testng.Assert
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import org.fest.assertions.Assertions
import java.lang.reflect.Field
import scala.reflect.runtime.{ universe => ru }
import scala.reflect.runtime.{ universe => ru }
import java.util.concurrent.atomic.AtomicReference

class BoundedBlockingQueueTest extends Assertions {

  val elementCounter = new AtomicInteger(0)

  @Test
  def shouldOfferElementToFullQueueUntilQueueIsCleared() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    verifyAndOfferAllElements(numberOfElements, queue);

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.clear()
    concurentPerformOfferElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldOfferElementToFullQueueUntilQueueIsDrained() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    verifyAndOfferAllElements(numberOfElements, queue);

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.drainTo(new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false))
    concurentPerformOfferElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldOfferElementToFullQueueUntilQueueIsDrainedUsingMaxElements() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    verifyAndOfferAllElements(numberOfElements, queue);

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.drainTo(new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false), numberOfElements * 2)
    concurentPerformOfferElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldOfferElementToFullQueueUntilAnotherElementIsRemoved() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    val firstElement = verifyAndOfferAllElements(numberOfElements, queue).iterator().next();

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.remove(firstElement)
    concurentPerformOfferElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldOfferElementToFullQueueUntilAnotherElementIsRemovedUsingIterator() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    val firstElement = verifyAndOfferAllElements(numberOfElements, queue).iterator().next();

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = {
      val iterator = queue.iterator();
      iterator.next();
      iterator.remove();
    }
    concurentPerformOfferElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldOfferElementToFullQueueUntilAnotherElementIsPolled() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    val firstElement = verifyAndOfferAllElements(numberOfElements, queue).iterator().next();

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.poll()
    concurentPerformOfferElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldOfferElementToFullQueueUntilAnotherElementIsPolledUsingSpecifiedTimeout() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    val firstElement = verifyAndOfferAllElements(numberOfElements, queue).iterator().next();

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.poll(10, TimeUnit.SECONDS)
    concurentPerformOfferElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldOfferElementToFullQueueUntilAnotherElementIsTaken() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    val firstElement = verifyAndOfferAllElements(numberOfElements, queue).iterator().next();

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.take()
    concurentPerformOfferElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldPutElementToFullQueueUntilQueueIsCleared() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    verifyAndOfferAllElements(numberOfElements, queue);

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.clear()
    concurrentPerformPutElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldPutElementToFullQueueUntilQueueIsDrained() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    verifyAndOfferAllElements(numberOfElements, queue);

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.drainTo(new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false));
    concurrentPerformPutElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldPutElementToFullQueueUntilQueueIsDrainedUsingMaxElements() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    verifyAndOfferAllElements(numberOfElements, queue);

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.drainTo(new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false), numberOfElements * 2);
    concurrentPerformPutElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldPutElementToFullQueueUntilAnotherElementIsRemoved() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    val firstElement = verifyAndOfferAllElements(numberOfElements, queue).iterator().next();

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.remove(firstElement)
    concurrentPerformPutElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldPutElementToFullQueueUntilAnotherElementIsRemovedUsingIterator() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    val firstElement = verifyAndOfferAllElements(numberOfElements, queue).iterator().next();

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = {
      val iterator = queue.iterator();
      iterator.next();
      iterator.remove();
    }
    concurrentPerformPutElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldPutElementToFullQueueUntilAnotherElementIsPolled() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    val firstElement = verifyAndOfferAllElements(numberOfElements, queue).iterator().next();

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.poll()
    concurrentPerformPutElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldPutElementToFullQueueUntilAnotherElementIsPolledUsingSpecifiedTimeout() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    val firstElement = verifyAndOfferAllElements(numberOfElements, queue).iterator().next();

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.poll(10, TimeUnit.SECONDS)
    concurrentPerformPutElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldPutElementToFullQueueUntilAnotherElementIsTaken() {
    val numberOfElements = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), numberOfElements, false);
    val firstElement = verifyAndOfferAllElements(numberOfElements, queue).iterator().next();

    assertThat(queue.remainingCapacity()).isZero();

    def enableSpace() = queue.take()
    concurrentPerformPutElementUntilSpaceIsAvailable(enableSpace, queue);
  }

  @Test
  def shouldPollElementFromEmptyQueueUntilElementIsOffered() {
    val capacity = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), capacity, false);
    val elementToAdd = new Element();

    assertThat(queue.remainingCapacity()).isEqualTo(capacity);

    def availableElement() = queue.offer(elementToAdd)
    concurrentPerformPollUntilElementIsAvailable(availableElement, queue, elementToAdd);
  }

  @Test
  def shouldPollElementFromEmptyQueueUntilElementIsOfferedUsingSpecifiedTimeout() {
    val capacity = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), capacity, false);
    val elementToAdd = new Element();

    assertThat(queue.remainingCapacity()).isEqualTo(capacity);

    def availableElement() = queue.offer(elementToAdd, 10, TimeUnit.SECONDS)
    concurrentPerformPollUntilElementIsAvailable(availableElement, queue, elementToAdd);
  }

  @Test
  def shouldPollElementFromEmptyQueueUntilElementIsPut() {
    val capacity = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), capacity, false);
    val elementToAdd = new Element();

    assertThat(queue.remainingCapacity()).isEqualTo(capacity);

    def availableElement() = queue.put(elementToAdd)
    concurrentPerformPollUntilElementIsAvailable(availableElement, queue, elementToAdd);
  }

  @Test
  def shouldPollElementFromEmptyQueueUntilElementIsAdded() {
    val capacity = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), capacity, false);
    val elementToAdd = new Element();

    assertThat(queue.remainingCapacity()).isEqualTo(capacity);

    def availableElement() = queue.add(elementToAdd)
    concurrentPerformPollUntilElementIsAvailable(availableElement, queue, elementToAdd);
  }

  @Test
  def shouldTakeElementFromEmptyQueueUntilElementIsOffered() {
    val capacity = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), capacity, false);
    val elementToAdd = new Element();

    assertThat(queue.remainingCapacity()).isEqualTo(capacity);

    def availableElement() = queue.offer(elementToAdd)
    concurrentPerformTakeUntilElementIsAvailable(availableElement, queue, elementToAdd);
  }

  @Test
  def shouldTakeElementFromEmptyQueueUntilElementIsOfferedUsingSpecifiedTimeout() {
    val capacity = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), capacity, false);
    val elementToAdd = new Element();

    assertThat(queue.remainingCapacity()).isEqualTo(capacity);

    def availableElement() = queue.offer(elementToAdd, 10, TimeUnit.SECONDS)
    concurrentPerformTakeUntilElementIsAvailable(availableElement, queue, elementToAdd);
  }

  @Test
  def shouldTakeElementFromEmptyQueueUntilElementIsPut() {
    val capacity = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), capacity, false);
    val elementToAdd = new Element();

    assertThat(queue.remainingCapacity()).isEqualTo(capacity);

    def availableElement() = queue.put(elementToAdd)
    concurrentPerformTakeUntilElementIsAvailable(availableElement, queue, elementToAdd);
  }

  @Test
  def shouldTakeElementFromEmptyQueueUntilElementIsAdd() {
    val capacity = 200;
    val queue = new BoundedBlockingQueue[Element](createQueueToWrap(), capacity, false);
    val elementToAdd = new Element();

    assertThat(queue.remainingCapacity()).isEqualTo(capacity);

    def availableElement() = queue.add(elementToAdd)
    concurrentPerformTakeUntilElementIsAvailable(availableElement, queue, elementToAdd);
  }

  private def concurentPerformOfferElementUntilSpaceIsAvailable(executionToEnableSpace: => Unit, queue: BlockingQueue[Element]) {
    val offeredElement = new AtomicBoolean(false)
    val reachedLockBarrier = new CountDownLatch(1)
    val offeredElementBarrier = new CountDownLatch(1)
    val internalLock = extractInternalLockFrom(queue)
    val elementToOffer = new Element()
    val execution = new Runnable {
      def run() {
        internalLock.lock()
        try {
          reachedLockBarrier.countDown()
          val isInserted = queue.offer(elementToOffer, 30, TimeUnit.SECONDS)
          offeredElement.set(isInserted)
          offeredElementBarrier.countDown()
        } finally internalLock.unlock()
      }
    };

    val executor = Executors.newCachedThreadPool()
    executor.execute(execution)

    reachedLockBarrier.await(10, TimeUnit.SECONDS)
    internalLock.lock()
    try executionToEnableSpace
    finally internalLock.unlock()

    offeredElementBarrier.await(2, TimeUnit.SECONDS)
    executor.shutdownNow()

    assertThat(offeredElement.get()).isTrue()
  }

  private def concurrentPerformPutElementUntilSpaceIsAvailable(executionToEnableSpace: => Unit, queue: BlockingQueue[Element]) {
    val putElement = new AtomicBoolean(false)
    val reachedLockBarrier = new CountDownLatch(1)
    val putElementBarrier = new CountDownLatch(1)
    val internalLock = extractInternalLockFrom(queue)
    val elementToOffer = new Element()
    val execution = new Runnable {
      def run() {
        internalLock.lock()
        try {
          reachedLockBarrier.countDown();
          queue.put(elementToOffer);
          putElement.set(true);
          putElementBarrier.countDown();
        } finally internalLock.unlock()
      }
    };

    val executor = Executors.newCachedThreadPool()
    executor.execute(execution)
    internalLock.lock()
    try executionToEnableSpace
    finally internalLock.unlock()

    putElementBarrier.await(2, TimeUnit.SECONDS)
    executor.shutdownNow()
    assertThat(putElement.get()).isTrue()
  }

  private def concurrentPerformPollUntilElementIsAvailable(executionToAddElementToQueue: => Unit, queue: BlockingQueue[Element], elementToBeAdded: Element) {
    val polledElement = new AtomicReference[Element]()
    val reachedLockBarrier = new CountDownLatch(1);
    val polledElementBarrier = new CountDownLatch(1);
    val internalLock = extractInternalLockFrom(queue);
    val execution = new Runnable {
      def run() {
        internalLock.lock()
        try {
          reachedLockBarrier.countDown();
          val element = queue.poll(30, TimeUnit.SECONDS);
          polledElement.set(element);
          polledElementBarrier.countDown();
        } finally internalLock.unlock()
      }
    };

    val executor = Executors.newCachedThreadPool()
    executor.execute(execution)
    internalLock.lock()
    try executionToAddElementToQueue
    finally internalLock.unlock()

    polledElementBarrier.await(2, TimeUnit.SECONDS);

    assertThat(polledElement.get()).isSameAs(elementToBeAdded);
  }

  private def concurrentPerformTakeUntilElementIsAvailable(executionToAddElementToQueue: => Unit, queue: BlockingQueue[Element], elementToBeAdded: Element) {
    val takenElement = new AtomicReference[Element]()
    val reachedLockBarrier = new CountDownLatch(1);
    val takenElementBarrier = new CountDownLatch(1);
    val internalLock = extractInternalLockFrom(queue);
    val execution = new Runnable {
      def run() {
        internalLock.lock()
        try {
          reachedLockBarrier.countDown();
          val element = queue.take();
          takenElement.set(element);
          takenElementBarrier.countDown();
        } finally internalLock.unlock()
      }
    };

    val executor = Executors.newCachedThreadPool()
    executor.execute(execution)
    internalLock.lock()
    try executionToAddElementToQueue
    finally internalLock.unlock()

    takenElementBarrier.await(2, TimeUnit.SECONDS);

    assertThat(takenElement.get()).isSameAs(elementToBeAdded);
  }

  private def extractInternalLockFrom(queue: BlockingQueue[Element]): ReentrantLock = {
    val field = queue.getClass.getDeclaredField("gakesson$util$concurrent$scala$BoundedBlockingQueue$$lock")
    field.setAccessible(true)
    field.get(queue).asInstanceOf[ReentrantLock]
  }

  private def verifyAndOfferAllElements(numberOfElements: Int, queue: BlockingQueue[Element]): ArrayList[Element] = {
    val createdElements = createElements(numberOfElements)
    @tailrec def offerOne(i: Int = 0) {
      if (i < createdElements.size()) {
        if (!queue.offer(createdElements.get(i))) {
          Assert.fail("Should have offered the specified element");
        }
        offerOne(i + 1)
      }
    }
    offerOne()
    createdElements
  }

  private def createElements(numberOfElements: Int): ArrayList[Element] = {
    val createdElements = new ArrayList[Element](numberOfElements)
    @tailrec def createOne(n: Int = 0) {
      if (n < numberOfElements) {
        createdElements.add(new Element())
        createOne(n + 1)
      }
    }
    createOne()
    Collections.shuffle(createdElements)
    createdElements
  }

  private def createQueueToWrap(): Queue[Element] = {
    new LinkedList[Element]();
  }

  class Element(elementId: Int = elementCounter.incrementAndGet()) {

    def getElementId(): Int = { elementId }
  }

  trait ExecutionInjection {
    def apply()
  }
}