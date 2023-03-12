package in.drongo.drongodb.util;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
/**
 * CircularList, A versatile datastructure next to graph.
 */
public class CircularDoublyLinkedList<E> implements Iterable<E>, Cloneable, Serializable {
    private static final long serialVersionUID = 1L;
    int size = 0;
    /**
     * Permanent Pointer to tail node.
     */
    transient CircularDoublyLinkedList.Node<E> tail;

    public CircularDoublyLinkedList() {
        tail = new CircularDoublyLinkedList.Node<E>();
    }

    public void addFirst(E e) {
        Node<E> tailHead = tail;
        Node<E> node = new Node<>(e);
        if (tailHead.next == null) {
            tailHead.next = node;
            node.next = tail;
            tailHead.prev = node;
            node.prev = tail;
        } else {
            Node<E> front = tailHead.next;
            tailHead.next = node;
            node.prev = tailHead;
            node.next = front;
            front.prev = node;
        }
        size = -~size;
    }

    public void removeFirst() {
        Node<E> tailHead = tail;
        if (tailHead.next != tailHead) {
            Node<E> front = tailHead.next;
            tailHead.next = front.next;
            front.next.prev = tailHead;
            size = ~-size;
        }
    }

    public void addLast(E e) {
        Node<E> tailHead = tail;
        Node<E> node = new Node<>(e);
        if (tailHead.prev == null) {
            tailHead.prev = node;
            node.prev = tail;
            tailHead.next = node;
            node.next = tail;
        } else {
            Node<E> front = tailHead.prev;
            tailHead.prev = node;
            node.next = tailHead;
            node.prev = front;
            front.next = node;
        }
        size = -~size;
    }

    public void removeLast() {
        Node<E> tailHead = tail;
        if (tailHead.prev != tailHead) {
            Node<E> front = tailHead.prev;
            tailHead.prev = front.prev;
            front.prev.next = tailHead;
            size = ~-size;
        }
    }

    public int size() {
        return size;
    }

    public E get(int index) {
        Node<E> tailRun = tail.next;
        for (int i = 0; i < size; i = -~i) {
            if (i == index) {
                return tailRun.item;
            }
            tailRun = tailRun.next;
        }
        throw new IndexOutOfBoundsException();
    }

    public Iterator<E> iterator() {
        return new IteratorImpl();
    }

    public Iterator<E> reverseterator() {
        return new ReverseIteratorImpl();
    }

    static class Node<E> {
        E item;
        Node<E> next;
        Node<E> prev;

        Node() {

        }

        Node(E element) {
            this.item = element;
        }

        public String toString() {
            return item.toString();
        }
    }

    private class IteratorImpl implements Iterator<E> {
        int position = 0;

        public boolean hasNext() {
            return position != size();
        }

        public E next() {
            try {
                E next = get(position);
                position = -~position;
                return next;
            } catch (IndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private class ReverseIteratorImpl implements Iterator<E> {
        int position = size() - 1;

        public boolean hasNext() {
            return position != -1;
        }

        public E next() {
            try {
                E next = get(position);
                position = ~-position;
                return next;
            } catch (IndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
