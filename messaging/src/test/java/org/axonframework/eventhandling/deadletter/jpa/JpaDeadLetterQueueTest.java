/*
 * Copyright (c) 2010-2022. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.eventhandling.deadletter.jpa;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.jpa.SimpleEntityManagerProvider;
import org.axonframework.common.transaction.Transaction;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.deadletter.EventHandlingQueueIdentifier;
import org.axonframework.lifecycle.Lifecycle;
import org.axonframework.messaging.deadletter.DeadLetterQueue;
import org.axonframework.messaging.deadletter.DeadLetterQueueTest;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class JpaDeadLetterQueueTest extends DeadLetterQueueTest<EventHandlingQueueIdentifier, EventMessage<?>> {

    private static final Duration EXPIRE_THRESHOLD = Duration.ofMillis(100);
    EntityManagerFactory emf = Persistence.createEntityManagerFactory("dlq");
    EntityManager entityManager = emf.createEntityManager();

    private final TransactionManager transactionManager = spy(new NoOpTransactionManager());
    private EntityTransaction transaction;

    @BeforeEach
    public void setUpJpa() throws SQLException {
        transaction = entityManager.getTransaction();
        transaction.begin();
    }

    @AfterEach
    public void rollback() {
        transaction.rollback();
    }

    @Override
    public DeadLetterQueue buildTestSubject() {
        EntityManagerProvider entityManagerProvider = new SimpleEntityManagerProvider(entityManager);
        return JpaDeadLetterQueue
                .builder()
                .transactionManager(transactionManager)
                .entityManagerProvider(entityManagerProvider)
                .expireThreshold(EXPIRE_THRESHOLD)
                .maxQueues(128)
                .build();
    }


    @Override
    public EventHandlingQueueIdentifier generateQueueId() {
        return generateQueueId(generateId());
    }

    @Override
    public EventHandlingQueueIdentifier generateQueueId(String group) {
        return new EventHandlingQueueIdentifier(generateId(), group);
    }

    @Override
    public EventMessage<?> generateMessage() {
        return GenericEventMessage.asEventMessage("Then this happened..." + UUID.randomUUID());
    }

    @Override
    public void setClock(Clock clock) {
        JpaDeadLetterQueue.clock = clock;
    }

    @Override
    public Duration expireThreshold() {
        return EXPIRE_THRESHOLD;
    }


    @Test
    void testMaxQueues() {
        int expectedMaxQueues = 128;

        JpaDeadLetterQueue<EventMessage<?>> testSubject = JpaDeadLetterQueue.builder()
                                                                                 .maxQueues(expectedMaxQueues)
                                                                                 .build();

        assertEquals(expectedMaxQueues, testSubject.maxQueues());
    }

    @Test
    void testMaxQueueSize() {
        int expectedMaxQueueSize = 128;

        JpaDeadLetterQueue<EventMessage<?>> testSubject = JpaDeadLetterQueue.builder()
                                                                                 .maxQueueSize(expectedMaxQueueSize)
                                                                                 .build();

        assertEquals(expectedMaxQueueSize, testSubject.maxQueueSize());
    }

    @Test
    void testRegisterLifecycleHandlerRegistersQueuesShutdown() {
        JpaDeadLetterQueue<EventMessage<?>> testSubject = spy(JpaDeadLetterQueue.defaultQueue());

        AtomicInteger onStartInvoked = new AtomicInteger(0);
        AtomicInteger onShutdownInvoked = new AtomicInteger(0);

        testSubject.registerLifecycleHandlers(new Lifecycle.LifecycleRegistry() {
            @Override
            public void onStart(int phase, @Nonnull Lifecycle.LifecycleHandler action) {
                onStartInvoked.incrementAndGet();
            }

            @Override
            public void onShutdown(int phase, @Nonnull Lifecycle.LifecycleHandler action) {
                onShutdownInvoked.incrementAndGet();
                action.run();
            }
        });

        assertEquals(0, onStartInvoked.get());
        assertEquals(1, onShutdownInvoked.get());
        verify(testSubject).shutdown();
    }

    @Test
    void testShutdownReturnsCompletedFutureForCustomizedExecutor() {
        ScheduledExecutorService scheduledExecutor = spy(Executors.newScheduledThreadPool(1));
        JpaDeadLetterQueue<EventMessage<?>> testSubject =
                JpaDeadLetterQueue.builder()
                                       .scheduledExecutorService(scheduledExecutor)
                                       .build();

        CompletableFuture<Void> result = testSubject.shutdown();

        assertTrue(result.isDone());
        verifyNoInteractions(scheduledExecutor);
    }

    @Test
    void testBuildDefaultQueue() {
        assertDoesNotThrow(() -> JpaDeadLetterQueue.defaultQueue());
    }

    @Test
    void testBuildWithNegativeMaxQueuesThrowsAxonConfigurationException() {
        JpaDeadLetterQueue.Builder<EventMessage<?>> builderTestSubject = JpaDeadLetterQueue.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.maxQueues(-1));
    }

    @Test
    void testBuildWithValueLowerThanMinimumMaxQueuesThrowsAxonConfigurationException() {
        IntStream.range(0, 127).forEach(i -> {
            JpaDeadLetterQueue.Builder<EventMessage<?>> builderTestSubject = JpaDeadLetterQueue.builder();

            assertThrows(AxonConfigurationException.class, () -> builderTestSubject.maxQueues(i));
        });
    }

    @Test
    void testBuildWithNegativeMaxQueueSizeThrowsAxonConfigurationException() {
        JpaDeadLetterQueue.Builder<EventMessage<?>> builderTestSubject = JpaDeadLetterQueue.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.maxQueueSize(-1));
    }

    @Test
    void testBuildWithValueLowerThanMinimumMaxQueueSizeThrowsAxonConfigurationException() {
        IntStream.range(0, 127).forEach(i -> {
            JpaDeadLetterQueue.Builder<EventMessage<?>> builderTestSubject = JpaDeadLetterQueue.builder();

            assertThrows(AxonConfigurationException.class, () -> builderTestSubject.maxQueueSize(i));
        });
    }

    @Test
    void testBuildWithNullExpireThresholdThrowsAxonConfigurationException() {
        JpaDeadLetterQueue.Builder<EventMessage<?>> builderTestSubject = JpaDeadLetterQueue.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.expireThreshold(null));
    }

    @Test
    void testBuildWithNegativeExpireThresholdThrowsAxonConfigurationException() {
        JpaDeadLetterQueue.Builder<EventMessage<?>> builderTestSubject = JpaDeadLetterQueue.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.expireThreshold(Duration.ofMillis(-1)));
    }

    @Test
    void testBuildWithZeroExpireThresholdThrowsAxonConfigurationException() {
        JpaDeadLetterQueue.Builder<EventMessage<?>> builderTestSubject = JpaDeadLetterQueue.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.expireThreshold(Duration.ZERO));
    }

    @Test
    void testBuildWithNullScheduledExecutorServiceThrowsAxonConfigurationException() {
        JpaDeadLetterQueue.Builder<EventMessage<?>> builderTestSubject = JpaDeadLetterQueue.builder();

        assertThrows(AxonConfigurationException.class, () -> builderTestSubject.scheduledExecutorService(null));
    }

    /**
     * A non-final {@link TransactionManager} implementation, so that it can be spied upon through Mockito.
     */
    private static class NoOpTransactionManager implements TransactionManager {

        @Override
        public Transaction startTransaction() {
            return new Transaction() {
                @Override
                public void commit() {
                    // No-op
                }

                @Override
                public void rollback() {
                    // No-op
                }
            };
        }
    }
}
