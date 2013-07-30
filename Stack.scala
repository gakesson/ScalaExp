package gakesson.util.concurrent.scala

trait Stack[E <: AnyRef] {
	def push(e: E);
	def pop(): E;
	def peek(): E;
	def isEmpty(): Boolean;
	def size(): Int;
}