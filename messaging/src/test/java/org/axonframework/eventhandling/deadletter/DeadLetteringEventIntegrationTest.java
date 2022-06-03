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

package org.axonframework.eventhandling.deadletter;

import org.axonframework.common.transaction.NoTransactionManager;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.StreamingEventProcessor;
import org.axonframework.eventhandling.pooled.PooledStreamingEventProcessor;
import org.axonframework.eventhandling.tokenstore.inmemory.InMemoryTokenStore;
import org.axonframework.messaging.annotation.MessageIdentifier;
import org.axonframework.messaging.deadletter.DeadLetter;
import org.axonframework.messaging.deadletter.DeadLetterQueue;
import org.axonframework.messaging.deadletter.QueueIdentifier;
import org.axonframework.messaging.unitofwork.RollbackConfigurationType;
import org.axonframework.utils.InMemoryStreamableEventSource;
import org.junit.jupiter.api.*;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.axonframework.utils.AssertUtils.assertWithin;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class validating the combination of an {@link org.axonframework.eventhandling.EventProcessor} containing a
 * {@link DeadLetteringEventHandlerInvoker} (a specific type of Processing Group). This test validates that:
 * <ul>
 *     <li>Handled {@link EventMessage EventMessages} are enqueued in a {@link DeadLetterQueue} if event handling fails.</li>
 *     <li>Handled {@link EventMessage EventMessages} are enqueued in a {@link DeadLetterQueue} if a previous event in that sequence was enqueued.</li>
 *     <li>Enqueued {@link EventMessage EventMessages} are successfully evaluated and removed from a {@link DeadLetterQueue}.</li>
 *     <li>Enqueued {@link EventMessage EventMessages} are unsuccessfully evaluated and enqueued in the {@link DeadLetterQueue} again.</li>
 * </ul>
 *
 * @author Steven van Beelen
 */
public abstract class DeadLetteringEventIntegrationTest {

    protected static final String PROCESSING_GROUP = "problematicProcessingGroup";
    protected static final boolean SUCCEED = true;
    protected static final boolean SUCCEED_RETRY = true;
    protected static final boolean FAIL = false;
    protected static final boolean FAIL_RETRY = false;

    protected ProblematicEventHandlingComponent eventHandlingComponent;
    protected DeadLetterQueue<EventMessage<?>> deadLetterQueue;
    protected DeadLetteringEventHandlerInvoker deadLetteringInvoker;
    protected InMemoryStreamableEventSource eventSource;
    protected StreamingEventProcessor streamingProcessor;

    /**
     * Constructs the {@link DeadLetterQueue} implementation used during the integration test.
     *
     * @return A {@link DeadLetterQueue} implementation used during the integration test.
     */
    protected abstract DeadLetterQueue<EventMessage<?>> buildDeadLetterQueue();

    @BeforeEach
    void setUp() {
        TransactionManager transactionManager = NoTransactionManager.instance();

        eventHandlingComponent = new ProblematicEventHandlingComponent();
        deadLetterQueue = buildDeadLetterQueue();
        deadLetteringInvoker = DeadLetteringEventHandlerInvoker.builder()
                                                               .eventHandlers(eventHandlingComponent)
                                                               .sequencingPolicy(event -> ((DeadLetterableEvent) event.getPayload()).getAggregateIdentifier())
                                                               .queue(deadLetterQueue)
                                                               .processingGroup(PROCESSING_GROUP)
                                                               .transactionManager(transactionManager)
                                                               .build();

        eventSource = new InMemoryStreamableEventSource();
        streamingProcessor = PooledStreamingEventProcessor.builder()
                                                          .name(PROCESSING_GROUP)
                                                          .eventHandlerInvoker(deadLetteringInvoker)
                                                          .rollbackConfiguration(RollbackConfigurationType.ANY_THROWABLE)
                                                          .messageSource(eventSource)
                                                          .tokenStore(new InMemoryTokenStore())
                                                          .transactionManager(transactionManager)
                                                          .coordinatorExecutor(Executors.newSingleThreadScheduledExecutor())
                                                          .workerExecutor(Executors.newSingleThreadScheduledExecutor())
                                                          .initialSegmentCount(1)
                                                          .claimExtensionThreshold(1000)
                                                          .build();
    }

    @AfterEach
    void tearDown() {
        CompletableFuture<Void> queueShutdown = deadLetterQueue.shutdown();
        CompletableFuture<Void> processorShutdown = streamingProcessor.shutdownAsync();
        try {
            CompletableFuture.allOf(queueShutdown, processorShutdown).get(15, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        }
    }

    protected void startProcessingEvent() {
        streamingProcessor.start();
    }

    protected void startDeadLetterEvaluation() {
        deadLetteringInvoker.start();
    }

    @Test
    void testFailedEventHandlingEnqueuesTheEvent() {
        eventSource.publishMessage(GenericEventMessage.asEventMessage(new DeadLetterableEvent("success", SUCCEED, 1)));
        eventSource.publishMessage(GenericEventMessage.asEventMessage(new DeadLetterableEvent("failure", FAIL, 2)));

        startProcessingEvent();

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, streamingProcessor.processingStatus().size()));
        //noinspection OptionalGetWithoutIsPresent
        assertWithin(
                1, TimeUnit.SECONDS,
                () -> assertTrue(streamingProcessor.processingStatus().get(0).getCurrentPosition().getAsLong() >= 2)
        );

        assertTrue(eventHandlingComponent.successfullyHandled("success"));
        assertTrue(eventHandlingComponent.unsuccessfullyHandled("failure"));

        assertTrue(deadLetterQueue.contains(new EventHandlingQueueIdentifier("failure", PROCESSING_GROUP)));
        assertFalse(deadLetterQueue.contains(new EventHandlingQueueIdentifier("success", PROCESSING_GROUP)));
    }

    @Test
    void testEventsInTheSameSequenceAreAllEnqueuedIfOneOfThemFails() {
        int expectedSuccessfulHandlingCount = 3;
        String aggregateId = UUID.randomUUID().toString();
        QueueIdentifier queueId = new EventHandlingQueueIdentifier(aggregateId, PROCESSING_GROUP);
        // Three events in sequence "aggregateId" succeed
        eventSource.publishMessage(GenericEventMessage.asEventMessage(new DeadLetterableEvent(aggregateId, SUCCEED, 1)));
        eventSource.publishMessage(GenericEventMessage.asEventMessage(new DeadLetterableEvent(aggregateId, SUCCEED, 2)));
        eventSource.publishMessage(GenericEventMessage.asEventMessage(new DeadLetterableEvent(aggregateId, SUCCEED, 3)));
        // On event in sequence "aggregateId" fails, causing the rest to fail
        DeadLetterableEvent firstDeadLetter = new DeadLetterableEvent(aggregateId, FAIL, 4);
        eventSource.publishMessage(GenericEventMessage.asEventMessage(firstDeadLetter));
        DeadLetterableEvent secondDeadLetter = new DeadLetterableEvent(aggregateId, SUCCEED, 5);
        eventSource.publishMessage(GenericEventMessage.asEventMessage(secondDeadLetter));
        DeadLetterableEvent thirdDeadLetter = new DeadLetterableEvent(aggregateId, SUCCEED, 6);
        eventSource.publishMessage(GenericEventMessage.asEventMessage(thirdDeadLetter));

        startProcessingEvent();

        assertWithin(25, TimeUnit.SECONDS, () -> assertEquals(1, streamingProcessor.processingStatus().size()));
//        noinspection OptionalGetWithoutIsPresent
        assertWithin(
                1, TimeUnit.SECONDS,
                () -> assertTrue(streamingProcessor.processingStatus().get(0).getCurrentPosition().getAsLong() >= 6)
        );

        assertTrue(eventHandlingComponent.successfullyHandled(aggregateId));
        assertEquals(expectedSuccessfulHandlingCount, eventHandlingComponent.successfulHandlingCount(aggregateId));
        assertTrue(eventHandlingComponent.unsuccessfullyHandled(aggregateId));
        assertEquals(1, eventHandlingComponent.unsuccessfulHandlingCount(aggregateId));

        assertTrue(deadLetterQueue.contains(queueId));

        // Release all entries so that they may be taken.
        deadLetterQueue.release();

        Optional<DeadLetter<EventMessage<?>>> first = deadLetterQueue.take(PROCESSING_GROUP);
        assertTrue(first.isPresent());
        assertLettersEqual(firstDeadLetter, first.get().message().getPayload());
        // Acknowledging removes the letter from the queue, allowing us to check the following letter
        first.get().acknowledge();
        Optional<DeadLetter<EventMessage<?>>> second = deadLetterQueue.take(PROCESSING_GROUP);
        assertTrue(second.isPresent());
        assertLettersEqual(secondDeadLetter, second.get().message().getPayload());
        second.get().acknowledge();
        Optional<DeadLetter<EventMessage<?>>> third = deadLetterQueue.take(PROCESSING_GROUP);
        assertTrue(third.isPresent());
        assertLettersEqual(thirdDeadLetter, third.get().message().getPayload());
        third.get().acknowledge();
        assertFalse(deadLetterQueue.contains(queueId));
    }

    private void assertLettersEqual(Object actual, Object expected) {
        assertInstanceOf(DeadLetterableEvent.class, actual);
        assertInstanceOf(DeadLetterableEvent.class, expected);
        DeadLetterableEvent actualEvent = (DeadLetterableEvent) actual;
        DeadLetterableEvent expectedEvent = (DeadLetterableEvent) expected;
        assertEquals(expectedEvent.getAggregateIdentifier(), actualEvent.getAggregateIdentifier());
        assertEquals(expectedEvent.getSequence(), actualEvent.getSequence());
    }

    @Test
    void testSuccessfulEvaluationRemovesTheDeadLetterFromTheQueue() {
        int expectedSuccessfulHandlingCount = 3;
        int expectedSuccessfulHandlingCountAfterEvaluation = 6;
        String aggregateId = UUID.randomUUID().toString();
        QueueIdentifier queueId = new EventHandlingQueueIdentifier(aggregateId, PROCESSING_GROUP);
        // Three events in sequence "aggregateId" succeed
        eventSource.publishMessage(GenericEventMessage.asEventMessage(new DeadLetterableEvent(aggregateId, SUCCEED, 1)));
        eventSource.publishMessage(GenericEventMessage.asEventMessage(new DeadLetterableEvent(aggregateId, SUCCEED, 2)));
        eventSource.publishMessage(GenericEventMessage.asEventMessage(new DeadLetterableEvent(aggregateId, SUCCEED, 3)));
        // On event in sequence "aggregateId" fails, causing the rest to fail, but succeed on a retry
        DeadLetterableEvent firstDeadLetter = new DeadLetterableEvent(aggregateId, 4, FAIL, SUCCEED_RETRY);
        eventSource.publishMessage(GenericEventMessage.asEventMessage(firstDeadLetter));
        DeadLetterableEvent secondDeadLetter = new DeadLetterableEvent(aggregateId, 5, SUCCEED, SUCCEED_RETRY);
        eventSource.publishMessage(GenericEventMessage.asEventMessage(secondDeadLetter));
        DeadLetterableEvent thirdDeadLetter = new DeadLetterableEvent(aggregateId, 6, SUCCEED, SUCCEED_RETRY);
        eventSource.publishMessage(GenericEventMessage.asEventMessage(thirdDeadLetter));

        startProcessingEvent();

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, streamingProcessor.processingStatus().size()));
        //noinspection OptionalGetWithoutIsPresent
        assertWithin(
                1, TimeUnit.SECONDS,
                () -> assertTrue(streamingProcessor.processingStatus().get(0).getCurrentPosition().getAsLong() >= 6)
        );

        assertTrue(eventHandlingComponent.successfullyHandled(aggregateId));
        assertEquals(expectedSuccessfulHandlingCount, eventHandlingComponent.successfulHandlingCount(aggregateId));
        assertTrue(eventHandlingComponent.unsuccessfullyHandled(aggregateId));
        assertEquals(1, eventHandlingComponent.unsuccessfulHandlingCount(aggregateId));

        assertTrue(deadLetterQueue.contains(queueId));

        startDeadLetterEvaluation();

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(
                expectedSuccessfulHandlingCountAfterEvaluation,
                eventHandlingComponent.successfulHandlingCount(aggregateId)
        ));
        assertWithin(1, TimeUnit.SECONDS, () -> assertFalse(deadLetterQueue.contains(queueId)));
    }

    @Test
    void testUnsuccessfulEvaluationRequeuesTheDeadLetterInTheQueue() {
        int expectedSuccessfulHandlingCount = 3;
        int expectedSuccessfulHandlingCountAfterEvaluation = 5;
        String aggregateId = UUID.randomUUID().toString();
        // Three events in sequence "aggregateId" succeed
        eventSource.publishMessage(GenericEventMessage.asEventMessage(new DeadLetterableEvent(aggregateId, SUCCEED, 1)));
        eventSource.publishMessage(GenericEventMessage.asEventMessage(new DeadLetterableEvent(aggregateId, SUCCEED, 2)));
        eventSource.publishMessage(GenericEventMessage.asEventMessage(new DeadLetterableEvent(aggregateId, SUCCEED, 3)));
        // On event in sequence "aggregateId" fails, causing the rest to fail, but...
        DeadLetterableEvent firstDeadLetter = new DeadLetterableEvent(aggregateId, 4, FAIL, SUCCEED_RETRY);
        eventSource.publishMessage(GenericEventMessage.asEventMessage(firstDeadLetter));
        DeadLetterableEvent secondDeadLetter = new DeadLetterableEvent(aggregateId, 5, SUCCEED, SUCCEED_RETRY);
        eventSource.publishMessage(GenericEventMessage.asEventMessage(secondDeadLetter));
        // ...the last retry fails.
        DeadLetterableEvent thirdDeadLetter = new DeadLetterableEvent(aggregateId, 6, SUCCEED, FAIL_RETRY);
        eventSource.publishMessage(GenericEventMessage.asEventMessage(thirdDeadLetter));

        startProcessingEvent();

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(1, streamingProcessor.processingStatus().size()));
        //noinspection OptionalGetWithoutIsPresent
        assertWithin(
                1, TimeUnit.SECONDS,
                () -> assertTrue(streamingProcessor.processingStatus().get(0).getCurrentPosition().getAsLong() >= 6)
        );

        assertTrue(eventHandlingComponent.successfullyHandled(aggregateId));
        assertEquals(expectedSuccessfulHandlingCount, eventHandlingComponent.successfulHandlingCount(aggregateId));
        assertTrue(eventHandlingComponent.unsuccessfullyHandled(aggregateId));
        assertEquals(1, eventHandlingComponent.unsuccessfulHandlingCount(aggregateId));

        assertTrue(deadLetterQueue.contains(new EventHandlingQueueIdentifier(aggregateId, PROCESSING_GROUP)));

        startDeadLetterEvaluation();

        assertWithin(1, TimeUnit.SECONDS, () -> assertEquals(
                expectedSuccessfulHandlingCountAfterEvaluation,
                eventHandlingComponent.successfulHandlingCount(aggregateId)
        ));
        assertWithin(1, TimeUnit.SECONDS,
                     () -> assertEquals(2, eventHandlingComponent.unsuccessfulHandlingCount(aggregateId)));
        assertWithin(1, TimeUnit.SECONDS, () -> {
            Optional<DeadLetter<EventMessage<?>>> requeuedLetter = deadLetterQueue.take(PROCESSING_GROUP);
            assertTrue(requeuedLetter.isPresent());
            DeadLetter<EventMessage<?>> result = requeuedLetter.get();
            assertLettersEqual(thirdDeadLetter, result.message().getPayload());
            assertEquals(1, result.numberOfRetries());
        });
    }

    // TODO: 09-05-22 concurrency tests

    protected static class ProblematicEventHandlingComponent {

        private final Map<String, Integer> successfullyHandled = new HashMap<>();
        private final Map<String, Integer> unsuccessfullyHandled = new HashMap<>();
        private final Set<String> shouldSucceedOnEvaluation = new HashSet<>();

        @EventHandler
        @Transactional
        public void on(DeadLetterableEvent event, @MessageIdentifier String eventIdentifier) {
            String aggregateIdentifier = event.getAggregateIdentifier();
            if ((event.shouldSucceed() && event.shouldSucceedOnEvaluation())
                    || shouldSucceedOnEvaluation.contains(eventIdentifier)) {
                successfullyHandled.compute(aggregateIdentifier, (id, count) -> count == null ? 1 : ++count);
            } else {
                if (event.shouldSucceedOnEvaluation()) {
                    shouldSucceedOnEvaluation.add(eventIdentifier);
                }
                unsuccessfullyHandled.compute(aggregateIdentifier, (id, count) -> count == null ? 1 : ++count);
                throw new RuntimeException("Let's dead-letter event [" + aggregateIdentifier + "]");
            }
        }

        public boolean successfullyHandled(String aggregateIdentifier) {
            return successfullyHandled.containsKey(aggregateIdentifier);
        }

        public int successfulHandlingCount(String aggregateIdentifier) {
            return successfullyHandled(aggregateIdentifier) ? successfullyHandled.get(aggregateIdentifier) : 0;
        }

        public boolean unsuccessfullyHandled(String aggregateIdentifier) {
            return unsuccessfullyHandled.containsKey(aggregateIdentifier);
        }

        public int unsuccessfulHandlingCount(String aggregateIdentifier) {
            return unsuccessfullyHandled(aggregateIdentifier) ? unsuccessfullyHandled.get(aggregateIdentifier) : 0;
        }
    }

    protected static class DeadLetterableEvent {

        private final String aggregateIdentifier;
        private final int sequence;
        private final boolean shouldSucceed;
        private final boolean shouldSucceedOnEvaluation;

        public DeadLetterableEvent(String aggregateIdentifier,
                                    boolean shouldSucceed, int sequence) {
            this(aggregateIdentifier, sequence, shouldSucceed, true);
        }

        public DeadLetterableEvent(String aggregateIdentifier,
                                    int sequence, boolean shouldSucceed,
                                    boolean shouldSucceedOnEvaluation) {
            this.aggregateIdentifier = aggregateIdentifier;
            this.sequence = sequence;
            this.shouldSucceed = shouldSucceed;
            this.shouldSucceedOnEvaluation = shouldSucceedOnEvaluation;
        }

        public String getAggregateIdentifier() {
            return aggregateIdentifier;
        }

        public boolean shouldSucceed() {
            return shouldSucceed;
        }

        public boolean shouldSucceedOnEvaluation() {
            return shouldSucceedOnEvaluation;
        }

        public int getSequence() {
            return sequence;
        }
    }
}
