package gakesson.util.concurrent.scala

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

class NonBlockingStack[E <: AnyRef] extends Stack[E] {
  private class Node(val element: E, val next: Node);
  private object Nil extends Node(null.asInstanceOf[E], null.asInstanceOf[Node]);

  private val top = new AtomicReference[Node](Nil)

  def push(e: E) {
    @tailrec def pushElement() {
      val current = top.get()
      val newTop = new Node(e, current)
      if (!top.compareAndSet(current, newTop)) pushElement()
    }
    pushElement()
  }

  def pop(): E = {
    @tailrec def popOne(): E = {
      val current = top.get()
      if (current ne Nil) {
        if (!top.compareAndSet(current, current.next)) popOne()
        else current.element
      } else null.asInstanceOf[E]
    }
    popOne()
  }
  
  def peek(): E = top.get().element

  def isEmpty(): Boolean = top.get() eq Nil

  def size(): Int = {
    @tailrec def traverse(node: Node = top.get(), count: Int = 0): Int = {
      if (node ne Nil) traverse(node.next, count + 1)
      else count
    }
    traverse()
  }
}