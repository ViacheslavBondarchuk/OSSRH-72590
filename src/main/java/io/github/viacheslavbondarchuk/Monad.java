package io.github.viacheslavbondarchuk;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.text.MessageFormat;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * author: viacheslavbondarchuk
 * time: 11:41 AM
 * date: 8/17/2021
 **/

@Slf4j
@EqualsAndHashCode
@SuppressWarnings("unchecked")
public class Monad<T, P> {
    private final Phaser phaser;
    private final T value;

    private Monad<T, P> previous;

    /**
     * @param monad
     */
    private Monad(Monad<T, P> monad) {
        this.value = monad.value;
        this.phaser = monad.phaser;
        this.previous = monad.previous;
    }

    /**
     * @param value
     * @param previous
     */
    public Monad(T value, Monad<T, P> previous) {
        this.value = value;
        this.phaser = null;
        this.previous = previous;
    }

    /**
     * @param value
     * @param phaser
     * @param previous
     */
    public Monad(T value, Phaser phaser, Monad<T, P> previous) {
        this.value = value;
        this.phaser = phaser;
        this.previous = previous;
    }

    private Monad() {
        this.value = null;
        this.phaser = null;
    }

    /**
     * @param <T>
     * @param <P>
     * @return
     */
    public static <T, P> Monad<T, P> empty() {
        return wrapOfNullable(null);
    }

    /**
     * @param <T>
     * @param <P>
     * @return
     */
    public static <T, P> Monad<T, P> emptyAsync() {
        return wrapAsyncOfNullable(null);
    }

    /**
     * @param value
     * @param <T>
     * @param <P>
     * @return
     */
    public static <T, P> Monad<T, P> wrap(T value) {
        Objects.requireNonNull(value);
        return new Monad<>(value, null);
    }

    /**
     * @param value
     * @param <T>
     * @param <P>
     * @return
     */
    public static <T, P> Monad<T, P> wrapAsync(T value) {
        Objects.requireNonNull(value);
        Phaser phaser = new Phaser();
        phaser.register();
        return new Monad<>(value, phaser, null);
    }

    /**
     * @param value
     * @param <T>
     * @param <P>
     * @return
     */
    public static <T, P> Monad<T, P> wrapOfNullable(T value) {
        return Objects.isNull(value) ? new Monad<>() : new Monad<>(value, null);
    }

    /**
     * @param value
     * @param <T>
     * @param <P>
     * @return
     */
    public static <T, P> Monad<T, P> wrapAsyncOfNullable(T value) {
        Phaser phaser = new Phaser();
        phaser.register();
        return Objects.isNull(value) ?
                new Monad<>(null, phaser, null)
                : new Monad<>(value, phaser, null);
    }

    /**
     * @param consumer
     * @return
     */
    public Monad<T, P> apply(Consumer<T> consumer) {
        if (Objects.nonNull(value))
            consumer.accept(value);
        return this;
    }

    /**
     * @param function
     * @param atomicReference
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAndStore(Function<T, R> function, AtomicReference<R> atomicReference) {
        if (Objects.nonNull(value))
            atomicReference.set(function.apply(value));
        return this;
    }

    /**
     * @param supplier
     * @param atomicReference
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyBothAndStore(Supplier<R> supplier, AtomicReference<R> atomicReference) {
        atomicReference.set(supplier.get());
        return this;
    }

    /**
     * @param function
     * @param consumer
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAndConsume(Function<T, R> function, Consumer<R> consumer) {
        if (Objects.nonNull(value))
            consumer.accept(function.apply(value));
        return this;
    }

    /**
     * @param supplier
     * @param consumer
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAndConsume(Supplier<R> supplier, Consumer<R> consumer) {
        if (Objects.nonNull(value))
            consumer.accept(supplier.get());
        return this;
    }

    /**
     * @param supplier
     * @param consumer
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyBothAndConsume(Supplier<R> supplier, Consumer<R> consumer) {
        consumer.accept(supplier.get());
        return this;
    }

    /**
     * @param operation
     * @return
     */
    public Monad<T, P> apply(Operation operation) {
        if (Objects.nonNull(value))
            operation.apply();
        return this;
    }

    /**
     * @param function
     * @param atomicReference
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAsyncAndStore(Function<T, R> function, AtomicReference<R> atomicReference) {
        if (Objects.nonNull(value)) {
            CompletableFuture.supplyAsync(() -> function.apply(value))
                    .handle(this::handleException)
                    .thenAccept(atomicReference::set);
        }
        return this;
    }

    /**
     * @param supplier
     * @param atomicReference
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyBothAsyncAndStore(Supplier<R> supplier, AtomicReference<R> atomicReference) {
        CompletableFuture.supplyAsync(() -> supplier.get())
                .handle(this::handleException)
                .thenAccept(atomicReference::set);
        return this;
    }

    /**
     * @param function
     * @param consumer
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAsyncAndConsume(Function<T, R> function, Consumer<R> consumer) {
        if (Objects.nonNull(value)) {
            CompletableFuture.supplyAsync(() -> function.apply(value))
                    .handle(this::handleException)
                    .thenAccept(consumer);
        }
        return this;
    }

    /**
     * @param supplier
     * @param consumer
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyBothAsyncAndConsume(Supplier<R> supplier, Consumer<R> consumer) {
        CompletableFuture.supplyAsync(supplier)
                .handle(this::handleException)
                .thenAccept(consumer);
        return this;
    }

    /**
     * @param supplier
     * @param consumer
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAsyncAndConsume(Supplier<R> supplier, Consumer<R> consumer) {
        if (Objects.nonNull(value)) {
            CompletableFuture.supplyAsync(supplier)
                    .handle(this::handleException)
                    .thenAccept(consumer);
        }
        return this;
    }

    /**
     * @param operation
     * @return
     */
    public Monad<T, P> applyAsync(Operation operation) {
        if (Objects.nonNull(value)) {
            CompletableFuture.runAsync(operation::apply)
                    .handle(this::handleException);
        }
        return this;
    }

    /**
     * @param operation
     * @return
     */
    public Monad<T, P> applyBothAsync(Operation operation) {
        CompletableFuture.runAsync(operation::apply)
                .handle(this::handleException);
        return this;
    }

    /**
     * @param operation
     * @return
     */
    public Monad<T, P> applyAsyncAndWaitOther(Operation operation) {
        if (Objects.nonNull(value)) {
            CompletableFuture.runAsync(() -> {
                        phaser.register();
                        operation.apply();
                    }).handle(this::handleException)
                    .thenRun(this::deregister);
        }
        return this;
    }

    /**
     * @param operation
     * @return
     */
    public Monad<T, P> applyBothAsyncAndWaitOther(Operation operation) {
        CompletableFuture.runAsync(() -> {
                    phaser.register();
                    operation.apply();
                }).handle(this::handleException)
                .thenRun(this::deregister);
        return this;
    }

    /**
     * @param consumer
     * @return
     */
    public Monad<T, P> applyAsync(Consumer<T> consumer) {
        if (Objects.nonNull(value)) {
            CompletableFuture.runAsync(() -> consumer.accept(value))
                    .handle(this::handleException);
        }
        return this;
    }

    /**
     * @param consumer
     * @return
     */
    public Monad<T, P> applyAsyncAndWaitOther(Consumer<T> consumer) {
        if (Objects.nonNull(value)) {
            CompletableFuture.runAsync(() -> {
                        phaser.register();
                        consumer.accept(value);
                    }).handle(this::handleException)
                    .thenRun(this::deregister);
        }
        return this;
    }

    /**
     * @param function
     * @param atomicReference
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAsyncWaitOtherAndStore(Function<T, R> function, AtomicReference<R> atomicReference) {
        if (Objects.nonNull(value)) {
            CompletableFuture.supplyAsync(() -> {
                        phaser.register();
                        return function.apply(value);
                    }).handle(this::handleException)
                    .thenAccept(atomicReference::set)
                    .thenRun(this::deregister);
        }
        return this;
    }

    /**
     * @param supplier
     * @param atomicReference
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyBothAsyncWaitOtherAndStore(Supplier<R> supplier, AtomicReference<R> atomicReference) {
        CompletableFuture.supplyAsync(() -> {
                    phaser.register();
                    return supplier.get();
                }).handle(this::handleException)
                .thenAccept(atomicReference::set)
                .thenRun(this::deregister);
        return this;
    }

    /**
     * @param function
     * @param consumer
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAsyncWaitOtherAndConsume(Function<T, R> function, Consumer<R> consumer) {
        if (Objects.nonNull(value)) {
            CompletableFuture.supplyAsync(() -> {
                        phaser.register();
                        return function.apply(value);
                    }).handle(this::handleException)
                    .thenAccept(consumer)
                    .thenRun(this::deregister);
        }
        return this;
    }

    /**
     * @param supplier
     * @param consumer
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyBothAsyncWaitOtherAndConsume(Supplier<R> supplier, Consumer<R> consumer) {
        CompletableFuture.supplyAsync(() -> {
                    phaser.register();
                    return supplier.get();
                }).handle(this::handleException)
                .thenAccept(consumer)
                .thenRun(this::deregister);
        return this;
    }

    /**
     * @param supplier
     * @param consumer
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAsyncWaitOtherAndConsume(Supplier<R> supplier, Consumer<R> consumer) {
        if (Objects.nonNull(value)) {
            CompletableFuture.supplyAsync(() -> {
                        phaser.register();
                        return supplier.get();
                    }).handle(this::handleException)
                    .thenAccept(consumer)
                    .thenRun(this::deregister);
        }
        return this;
    }

    /**
     * @param supplier
     * @param atomicReference
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyIfNullAndStore(Supplier<R> supplier, AtomicReference<R> atomicReference) {
        if (Objects.isNull(value))
            atomicReference.set(supplier.get());
        return this;
    }

    /**
     * @param supplier
     * @param consumer
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyIfNullAndConsume(Supplier<R> supplier, Consumer<R> consumer) {
        if (Objects.isNull(value))
            consumer.accept(supplier.get());
        return this;
    }

    /**
     * @param operation
     * @return
     */
    public Monad<T, P> applyIfNull(Operation operation) {
        if (Objects.isNull(value))
            operation.apply();
        return this;
    }

    /**
     * @param operation
     * @return
     */
    public Monad<T, P> applyAsyncIfNull(Operation operation) {
        if (Objects.isNull(value)) {
            CompletableFuture.runAsync(operation::apply)
                    .handle(this::handleException);
        }
        return this;
    }

    /**
     * @param supplier
     * @param atomicReference
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAsyncIfNullAndStore(Supplier<R> supplier, AtomicReference<R> atomicReference) {
        if (Objects.isNull(value))
            CompletableFuture.supplyAsync(supplier)
                    .handle(this::handleException)
                    .thenAccept(atomicReference::set);
        return this;
    }

    /**
     * @param supplier
     * @param consumer
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAsyncIfNullAndConsume(Supplier<R> supplier, Consumer<R> consumer) {
        if (Objects.isNull(value)) {
            CompletableFuture.supplyAsync(supplier)
                    .handle(this::handleException)
                    .thenAccept(consumer);
        }
        return this;
    }

    /**
     * @param consumer
     * @return
     */
    public Monad<T, P> applyAsyncIfNull(Consumer<T> consumer) {
        if (Objects.isNull(value)) {
            CompletableFuture.runAsync(() -> consumer.accept(value))
                    .handle(this::handleException);
        }
        return this;
    }

    /**
     * @param consumer
     * @return
     */
    public Monad<T, P> applyAsyncIfNullAndWaitOther(Consumer<T> consumer) {
        if (Objects.isNull(value)) {
            CompletableFuture.runAsync(() -> {
                        phaser.register();
                        consumer.accept(value);
                    }).handle(this::handleException)
                    .thenRun(this::deregister);
        }
        return this;
    }

    /**
     * @param operation
     * @return
     */
    public Monad<T, P> applyAsyncIfNullAndWaitOther(Operation operation) {
        if (Objects.isNull(value)) {
            CompletableFuture.runAsync(() -> {
                        phaser.register();
                        operation.apply();
                    }).handle(this::handleException)
                    .thenRun(this::deregister);
        }
        return this;
    }

    /**
     * @param supplier
     * @param atomicReference
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAsyncIfNullWaitOtherAndStore(Supplier<R> supplier, AtomicReference<R> atomicReference) {
        if (Objects.isNull(value)) {
            CompletableFuture.supplyAsync(() -> {
                        phaser.register();
                        return supplier.get();
                    }).handle(this::handleException)
                    .thenAccept(atomicReference::set)
                    .thenRun(this::deregister);
        }
        return this;
    }

    /**
     * @param supplier
     * @param consumer
     * @param <R>
     * @return
     */
    public <R> Monad<T, P> applyAsyncIfNullWaitOtherAndConsume(Supplier<R> supplier, Consumer<R> consumer) {
        if (Objects.isNull(value)) {
            CompletableFuture.supplyAsync(() -> {
                        phaser.register();
                        return supplier.get();
                    }).handle(this::handleException)
                    .thenAccept(consumer)
                    .thenRun(this::deregister);
        }
        return this;
    }

    /**
     * @param function
     * @param <M>
     * @return
     */
    public <M> Monad<M, T> mutable(Function<T, M> function) {
        if (Objects.isNull(phaser) && Objects.isNull(value)) {
            return new Monad<M, T>(null, (Monad<M, T>) this);
        }
        if (Objects.nonNull(phaser) && Objects.isNull(value)) {
            return new Monad<M, T>(null, phaser, (Monad<M, T>) this);
        }
        if (Objects.isNull(phaser) && Objects.nonNull(value)) {
            return new Monad<M, T>(function.apply(value), (Monad<M, T>) this);
        }
        return new Monad<M, T>(function.apply(value), phaser, (Monad<M, T>) this);
    }

    /**
     * @param supplier
     * @param <M>
     * @return
     */
    public <M> Monad<M, T> mutableIfNull(Supplier<M> supplier) {
        return Objects.isNull(phaser) ?
                new Monad<M, T>(supplier.get(), (Monad<M, T>) this)
                : new Monad<M, T>(supplier.get(), phaser, (Monad<M, T>) this);

    }

    /**
     * @param result
     * @param <R>
     * @return
     */
    public <R extends T> T orElse(R result) {
        if (Objects.isNull(value)) {
            return result;
        }
        return value;
    }

    /**
     * @param exceptionSupplier
     * @param <X>
     * @return
     * @throws X
     */
    public <X extends Throwable> Monad<T, P> orElseThrow(Supplier<? extends X> exceptionSupplier) throws X {
        if (Objects.isNull(value))
            throw exceptionSupplier.get();
        return this;
    }

    /**
     * Don`value use this method in the end of monad chain, use {@link #unwrap()} method
     */
    public Monad<T, P> await() {
        if (Objects.nonNull(phaser) && phaser.getRegisteredParties() > 0) {
            phaser.arriveAndAwaitAdvance();
        }
        return this;
    }

    /**
     * @return
     */
    public Monad<T, P> printPhase() {
        if (log.isDebugEnabled() && Objects.nonNull(phaser))
            log.debug("Current phase of barrier: {}", phaser.getPhase());
        return this;
    }

    public Monad<T, P> spliterator() {
        return this;
    }

    private void deregister() {
        if (log.isDebugEnabled())
            log.debug("Thread: {}, did arrive and deregister", Thread.currentThread().getName());
        phaser.arriveAndDeregister();
    }

    private <V, X extends Throwable> V handleException(V value, X ex) {
        if (Objects.nonNull(ex))
            log.error(ex.getMessage(), ex);
        return value;
    }

    public Monad<P, T> rollback() {
        Monad<P, T> monad = (Monad<P, T>) new Monad<>(previous);
        this.previous = this;
        return monad;
    }

    public T unwrap() {
        if (Objects.nonNull(phaser) && phaser.getRegisteredParties() > 0) {
            phaser.arriveAndAwaitAdvance();
            phaser.forceTermination();
        }
        return value;
    }

    @FunctionalInterface
    public interface Operation {
        void apply();
    }

}
