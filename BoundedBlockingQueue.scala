package gakesson.util.concurrent.scala

import java.util.AbstractQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.TimeUnit
import java.util.Queue
import java.util.Collection
import java.util.Iterator
import java.util.concurrent.locks.Condition
import scala.annotation.tailrec

class BoundedBlockingQueue[E <: AnyRef](private val backingQueue: Queue[E], private val capacity: Int, private val fair: Boolean)
  extends AbstractQueue[E] with BlockingQueue[E] {

  backingQueue match {
  case null => throw new IllegalArgumentException  
  case b: Queue[_] => {
      require(b.size() <= capacity)
      require(capacity > 0)
    }
  }

  private val lock = new ReentrantLock(fair)
  private val notEmpty = lock.newCondition()
  private val notFull = lock.newCondition()

  @throws[InterruptedException]
  def put(e: E) {
    if (e eq null) throw new NullPointerException
    lock.lockInterruptibly()
    try {
      @tailrec def putOne(e: E) {
        if (backingQueue.size() < capacity) {
          backingQueue.add(e)
          notEmpty.signal()
        } else {
          notFull.await()
          putOne(e)
        }
      }
      putOne(e)
    } finally lock.unlock();
  }

  def offer(e: E): Boolean = {
    if (e eq null) throw new NullPointerException
    lock.lock()
    try {
      if (backingQueue.size() < capacity) {
        backingQueue.add(e)
        notEmpty.signal()
        true
      } else false
    } finally lock.unlock()
  }

  @throws[InterruptedException]
  def offer(e: E, timeout: Long, unit: TimeUnit): Boolean = {
    if (e eq null) throw new NullPointerException
    val timeoutNanos = unit.toNanos(timeout)
    lock.lockInterruptibly()
    try {
      @tailrec def offerOne(e: E, timeoutNanos: Long): Boolean = {
        if (backingQueue.size() < capacity) {
          backingQueue.add(e)
          notEmpty.signal()
          true
        } else if (timeoutNanos > 0) offerOne(e, notFull.awaitNanos(timeoutNanos))
        else false
      }
      offerOne(e, timeoutNanos)
    } finally lock.unlock()
  }

  @throws[InterruptedException]
  def take(): E = {
    lock.lockInterruptibly()
    try {
      @tailrec def takeOne(): E = {
        if (!backingQueue.isEmpty()) {
          val e = backingQueue.remove()
          notFull.signal()
          e
        } else {
          notEmpty.await()
          takeOne()
        }
      }
      takeOne()
    } finally lock.unlock()
  }

  def poll(): E = {
    lock.lock()
    try {
      backingQueue.poll() match {
        case null => null.asInstanceOf[E]
        case e =>
          notFull.signal()
          e
      }
    } finally lock.unlock()
  }

  @throws[InterruptedException]
  def poll(timeout: Long, unit: TimeUnit): E = {
    val timeoutNanos = unit.toNanos(timeout)
    lock.lockInterruptibly()
    try {
      @tailrec def pollOne(timeoutNanos: Long): E = {
        if (!backingQueue.isEmpty()) {
          backingQueue.poll() match {
            case null => null.asInstanceOf[E]
            case e =>
              notFull.signal()
              e
          }
        } else if (timeoutNanos > 0) pollOne(notEmpty.awaitNanos(timeoutNanos))
        else null.asInstanceOf[E]
      }
      pollOne(timeoutNanos)
    } finally lock.unlock()
  }

  def peek(): E = {
    lock.lock()
    try {
      backingQueue.peek()
    } finally lock.unlock()
  }

  def remainingCapacity(): Int = {
    lock.lock()
    try {
      capacity - size()
    } finally lock.unlock()
  }

  override def remove(o: AnyRef): Boolean = {
    if (o eq null) throw new NullPointerException
    lock.lock()
    try {
      backingQueue.remove(o) match {
        case false => false
        case true =>
          notFull.signal()
          true
      }
    } finally lock.unlock()
  }

  override def contains(o: AnyRef): Boolean = {
    if (o eq null) throw new NullPointerException
    lock.lock()
    try {
      backingQueue.contains(o)
    } finally lock.unlock()
  }

  def size(): Int = {
    lock.lock()
    try {
      backingQueue.size()
    } finally lock.unlock()
  }

  override def clear() {
    lock.lock()
    try {
      backingQueue.clear()
      notFull.signalAll()
    } finally lock.unlock()
  }

  def drainTo(c: Collection[_ >: E]): Int = drainTo(c, Int.MaxValue)

  def drainTo(c: Collection[_ >: E], maxElements: Int): Int = {
    if (c eq null) throw new NullPointerException
    require(c ne this)
    require(c ne backingQueue)
    lock.lock()
    try {
      @tailrec def drainOne(n: Int = 0): Int = {
        if (n < maxElements && !backingQueue.isEmpty()) {
          c.add(backingQueue.remove())
          drainOne(n + 1)
        } else n
      }
      val n = drainOne()
      if (n > 0) notFull.signalAll()
      n
    } finally lock.unlock()
  }

  def iterator(): Iterator[E] = {
    lock.lock()
    try {
      val queueElements = backingQueue.toArray()
      new Iterator[E] {
        private def NO_INDEX: Int = { -1 }

        private var currentIndex = 0
        private var lastReturnedIndex = NO_INDEX

        def hasNext(): Boolean = {
          currentIndex < queueElements.length
        }

        def next(): E = {
          if (!hasNext()) throw new NoSuchElementException
          val element = queueElements(currentIndex)
          lastReturnedIndex = currentIndex
          currentIndex = currentIndex + 1
          element.asInstanceOf[E]
        }

        def remove() {
          if (lastReturnedIndex == NO_INDEX) throw new IllegalStateException
          val removeIndex = lastReturnedIndex
          lastReturnedIndex = NO_INDEX
          def removeElementByIdentity(o: AnyRef) {
            lock.lock()
            try {
              @tailrec def removeTarget(queueIterator: Iterator[E] = backingQueue.iterator()) {
                if (queueIterator.hasNext()) {
                  if (queueIterator.next() eq o) {
                    queueIterator.remove()
                    notFull.signal()
                  } else removeTarget(queueIterator)
                }
              }
              removeTarget()
            } finally lock.unlock()
          }
          removeElementByIdentity(queueElements(removeIndex))
        }
      }
    } finally lock.unlock()
  }
}