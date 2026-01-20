package ai.pipestream.module.chunker.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A simple buffer for capturing processed documents during test runs.
 * This is a local implementation that replaces the old pipestream-commons ProcessingBuffer.
 *
 * @param <T> The type of items stored in the buffer
 */
public class ProcessingBuffer<T> {

    private final boolean enabled;
    private final int capacity;
    private final ConcurrentLinkedQueue<T> items;

    public ProcessingBuffer(boolean enabled, int capacity) {
        this.enabled = enabled;
        this.capacity = capacity;
        this.items = new ConcurrentLinkedQueue<>();
    }

    /**
     * Add an item to the buffer if enabled.
     *
     * @param item the item to add
     * @return true if item was added, false if buffer is disabled or full
     */
    public boolean add(T item) {
        if (!enabled) {
            return false;
        }
        if (items.size() >= capacity) {
            items.poll(); // Remove oldest item
        }
        return items.offer(item);
    }

    /**
     * Get all items in the buffer.
     *
     * @return an unmodifiable list of all items
     */
    public List<T> getItems() {
        return Collections.unmodifiableList(new ArrayList<>(items));
    }

    /**
     * Clear all items from the buffer.
     */
    public void clear() {
        items.clear();
    }

    /**
     * Check if the buffer is enabled.
     *
     * @return true if enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Get the current number of items in the buffer.
     *
     * @return the number of items
     */
    public int size() {
        return items.size();
    }

    /**
     * Get the maximum capacity of the buffer.
     *
     * @return the capacity
     */
    public int getCapacity() {
        return capacity;
    }
}
