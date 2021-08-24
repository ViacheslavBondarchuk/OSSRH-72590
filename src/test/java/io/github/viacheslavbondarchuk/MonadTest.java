package io.github.viacheslavbondarchuk;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * author: viacheslavbondarchuk
 * date: 8/24/21
 * time: 9:54 PM
 */
public class MonadTest {
    private final static String TEST_VALUE = "HELLO";

    @SneakyThrows
    private void sleep(long timeout) {
        Thread.sleep(timeout);
    }

    private void sleep2() {
        sleep(2000);
    }

    private void sleep4() {
        sleep(4000);
    }

    @Test
    @Timeout(value = 6)
    public void testOnDeadLockWithNull() {
        Monad.wrapAsyncOfNullable((String) null)
                .applyAsyncIfNullAndWaitOther(this::sleep2)
                .applyAsyncIfNullAndWaitOther(this::sleep4)
                .unwrap();
    }

    @Test
    @Timeout(value = 6)
    public void testOnDeadLockWithValue() {
        Monad.wrapAsyncOfNullable(TEST_VALUE)
                .applyAsyncAndWaitOther(this::sleep2)
                .applyAsyncAndWaitOther(this::sleep4)
                .unwrap();
    }

    @Test
    @Timeout(value = 12)
    public void testOnDeadLockWithMutableAndNull() {
        Monad.wrapAsyncOfNullable(TEST_VALUE)
                .applyAsyncAndWaitOther(this::sleep2)
                .applyAsyncAndWaitOther(this::sleep4)
                .mutable((v) -> null)
                .applyAsyncIfNullAndWaitOther(this::sleep2)
                .applyAsyncIfNullAndWaitOther(this::sleep4)
                .unwrap();
    }

    @Test
    @Timeout(value = 12)
    public void testOnDeadLockWithMutableAndValue() {
        Monad.wrapAsyncOfNullable(TEST_VALUE)
                .applyAsyncAndWaitOther(this::sleep2)
                .applyAsyncAndWaitOther(this::sleep4)
                .mutable(String::chars)
                .applyAsyncAndWaitOther(this::sleep2)
                .applyAsyncAndWaitOther(this::sleep4)
                .unwrap();
    }

    @Test
    @Timeout(value = 12)
    public void testOnDeadLockWithMutableAndRollback() {
        String hello = Monad.wrapAsyncOfNullable(TEST_VALUE)
                .applyAsyncAndWaitOther(this::sleep2)
                .applyAsyncAndWaitOther(this::sleep4)
                .mutable(String::chars)
                .applyAsyncAndWaitOther(this::sleep2)
                .applyAsyncAndWaitOther(this::sleep4)
                .rollback()
                .applyAsyncAndWaitOther(this::sleep2)
                .applyAsyncAndWaitOther(this::sleep4)
                .unwrap();
        Assertions.assertEquals(TEST_VALUE, hello);
    }

    @Test
    @Timeout(value = 18)
    public void testOnDeadLockWithMutableAndRollbackAndAwait() {
        String hello = Monad.wrapAsyncOfNullable(TEST_VALUE)
                .printPhase()
                .applyAsyncAndWaitOther(this::sleep2)
                .applyAsyncAndWaitOther(this::sleep4)
                .await()
                .printPhase()
                .mutable(String::chars)
                .applyAsyncAndWaitOther(this::sleep2)
                .applyAsyncAndWaitOther(this::sleep4)
                .await()
                .printPhase()
                .rollback()
                .applyAsyncAndWaitOther(this::sleep2)
                .applyAsyncAndWaitOther(this::sleep4)
                .unwrap();
        Assertions.assertEquals(TEST_VALUE, hello);
    }

    @Test
    @Timeout(value = 12)
    public void testOnDeadLockWithException() {
        Monad.wrapAsyncOfNullable(TEST_VALUE)
                .applyAsyncAndWaitOther(this::sleep2)
                .applyAsyncAndWaitOther(this::sleep4)
                .applyAsyncAndWaitOther(() -> {throw new RuntimeException("TEST EXCEPTION");})
                .mutable(String::chars)
                .applyAsyncAndWaitOther(this::sleep2)
                .applyAsyncAndWaitOther(this::sleep4)
                .unwrap();
    }

}
