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

import com.thoughtworks.xstream.XStream;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.AxonThreadFactory;
import org.axonframework.common.IdentifierFactory;
import org.axonframework.common.jpa.EntityManagerProvider;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.EventUtils;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.deadletter.DeadLetter;
import org.axonframework.messaging.deadletter.DeadLetterEvaluationException;
import org.axonframework.messaging.deadletter.DeadLetterQueue;
import org.axonframework.messaging.deadletter.DeadLetterQueueOverflowException;
import org.axonframework.messaging.deadletter.GenericCause;
import org.axonframework.messaging.deadletter.GenericDeadLetter;
import org.axonframework.messaging.deadletter.QueueIdentifier;
import org.axonframework.messaging.deadletter.SchedulingDeadLetterQueue;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.upcasting.event.EventUpcaster;
import org.axonframework.serialization.upcasting.event.NoOpEventUpcaster;
import org.axonframework.serialization.xml.CompactDriver;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.common.BuilderUtils.assertThat;

/**
 * JPA implementation of the {@link DeadLetterQueue}.
 * <p>
 * Dead letters are serialized and put into a specific table for the DLQ. This is mapped by the {@link DeadLetterEntry}
 * JPA entity. When reading the {@link DeadLetterEntry} from the table, it's deserialized and the configured
 * {@link EventUpcaster} is applied so dead letters can be fixed by creating in this manner. Since the events are
 * retried on a one-by-one basis, context aware upcasters will not function correctly.
 * <p>
 * There are size limits enforced. By default, a maximum of {@code 1024} unique {@link QueueIdentifier}s are allowed to
 * be in the dead letter queue. Within these unique {@link QueueIdentifier}s a maximum of {@code 1024} messages are
 * allowed to be queued by default. These default can be configured through {@link Builder#maxQueueSize(int)} and
 * {@link Builder#maxQueues(int)}.
 *
 * @param <T> The type of {@link EventMessage} maintained in this {@link DeadLetterQueue}.
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
public class JpaDeadLetterQueue<T extends EventMessage<?>> extends SchedulingDeadLetterQueue<T> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * {@link Clock} instance used to set the time on new {@link DeadLetter}s. To fix the time while testing set this
     * value to a constant value.
     */
    public static Clock clock = Clock.systemUTC();


    private final EntityManagerProvider entityManagerProvider;
    private final TransactionManager transactionManager;

    private final int maxQueues;
    private final int maxQueueSize;

    private final Serializer serializer;
    private final EventUpcaster upcasterChain;

    /**
     * Instantiate a {@link JpaDeadLetterQueue} based on the given {@link Builder builder}.
     *
     * @param builder The {@link Builder} used to instantiate a {@link JpaDeadLetterQueue} instance.
     */
    protected JpaDeadLetterQueue(Builder<T> builder) {
        super(builder);
        builder.validate();
        this.maxQueues = builder.maxQueues;
        this.maxQueueSize = builder.maxQueueSize;
        this.entityManagerProvider = builder.entityManagerProvider;
        this.transactionManager = builder.transactionManager;
        this.serializer = builder.serializer;
        this.upcasterChain = builder.upcasterChain;
    }

    /**
     * Instantiate a builder to construct an {@link JpaDeadLetterQueue}.
     * <p>
     * The maximum number of unique {@link QueueIdentifier}s defaults to {@code 1024}, the maximum amount of dead
     * letters inside a {@link QueueIdentifier} defaults to {@code 1024}, the dead letter expire threshold defaults to a
     * {@link Duration} of 5000 milliseconds, and the {@link ScheduledExecutorService} defaults to a
     * {@link Executors#newSingleThreadScheduledExecutor(ThreadFactory)}, using an {@link AxonThreadFactory}.
     *
     * @param <T> The type of {@link EventMessage} maintained in this {@link DeadLetterQueue}.
     * @return A Builder that can construct an {@link JpaDeadLetterQueue}.
     */
    public static <T extends EventMessage<?>> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Construct a default {@link JpaDeadLetterQueue}.
     * <p>
     * The maximum number of unique {@link QueueIdentifier}s defaults to {@code 1024}, the maximum amount of dead
     * letters inside a unique {@link QueueIdentifier}s defaults to {@code 1024}, the dead letter expire threshold
     * defaults to a {@link Duration} of 5000 milliseconds, and the {@link ScheduledExecutorService} defaults to a
     * {@link Executors#newSingleThreadScheduledExecutor(ThreadFactory)}, using an {@link AxonThreadFactory}.
     *
     * @param <T> The type of {@link EventMessage} maintained in this {@link DeadLetterQueue}.
     * @return A default {@link JpaDeadLetterQueue}.
     */
    public static <T extends EventMessage<?>> JpaDeadLetterQueue<T> defaultQueue() {
        //noinspection unchecked
        return (JpaDeadLetterQueue<T>) builder().build();
    }

    @Override
    public DeadLetter<T> enqueue(@Nonnull QueueIdentifier identifier,
                                 @Nonnull T deadLetter,
                                 Throwable cause) throws DeadLetterQueueOverflowException {
        if (isFull(identifier)) {
            throw new DeadLetterQueueOverflowException(
                    "No room left to enqueue message [" + deadLetter + "] for identifier ["
                            + identifier.combinedIdentifier() + "] since the queue is full.",
                    cause
            );
        }

        if (cause != null) {
            logger.debug("Adding dead letter [{}] because [{}].", deadLetter, cause);
        } else {
            logger.debug("Adding dead letter [{}] because the queue identifier [{}] is already present.",
                         deadLetter, identifier);
        }

        Instant deadLettered = clock.instant();
        Instant expiresAt = cause == null ? deadLettered : deadLettered.plus(expireThreshold);
        DeadLetterEntry jpaEntry = new DeadLetterEntry(IdentifierFactory.getInstance().generateIdentifier(),
                                                       toJpaIdentifier(identifier),
                                                       getNextIndexForQueueIdentifier(identifier),
                                                       deadLetter,
                                                       serializer,
                                                       deadLettered,
                                                       expiresAt,
                                                       cause != null ? new GenericCause(cause) : null
        );

        transactionManager.executeInTransaction(() -> {
            entityManager().persist(jpaEntry);
            entityManager().flush();
        });
        dlqCache().ifPresent(Map::clear);
        scheduleAvailabilityCallbacks(identifier);

        return toDeadLetter(jpaEntry);
    }

    /**
     * Method to retrieve the next DLQ index for this {@link QueueIdentifier}. Will return 0 if no messages are yet
     * present for this {@link QueueIdentifier}, otherwise will return the current maximum + 1.
     * <p>
     * The index is used to preserve message ordering within a {@link QueueIdentifier}.
     *
     * @param identifier The identifier to get the next index for.
     * @return The index for the next message put on the DLQ for this {@link QueueIdentifier}.
     */
    private int getNextIndexForQueueIdentifier(@Nonnull QueueIdentifier identifier) {
        Integer maxIndexForQueueIdentifier = getMaxIndexForQueueIdentifier(identifier);
        if (maxIndexForQueueIdentifier == null) {
            return 0;
        }
        return maxIndexForQueueIdentifier + 1;
    }

    /**
     * Fetches the current maximum index for a queue identifier. Messages which are enqueued next should have an index
     * higher than the one returned. If the query returns null it indicates that the queue is empty.
     *
     * @param identifier The identifier of the queue to check the index for.
     */
    private Integer getMaxIndexForQueueIdentifier(@Nonnull QueueIdentifier identifier) {
        EntityManager entityManager = entityManagerProvider.getEntityManager();
        return transactionManager.fetchInTransaction(() -> {
            try {
                return entityManager.createQuery(
                                            "SELECT max(dl.index) FROM DeadLetterEntry dl where dl.identifier.queueGroup=:group and dl.identifier.identifier=:identifier",
                                            Integer.class
                                    )
                                    .setParameter("identifier", identifier.identifier())
                                    .setParameter("group", identifier.group())
                                    .getSingleResult();
            } catch (NoResultException e) {
                // Expected, queue is empty. Return null.
                return null;
            }
        });
    }

    /**
     * Creates a {@link GenericDeadLetter} out of the provided JPA entity. Uses the provided
     * {@link org.axonframework.serialization.upcasting.Upcaster} to make sure the {@link EventMessage} is upcasted for
     * the caller.
     *
     * @param entry The {@link DeadLetterEntry} to upcast and deserialize.
     * @return The {@link DeadLetter} instanced containing the upcasted {@link EventMessage} as message.
     */
    @SuppressWarnings("unchecked")
    private DeadLetter<T> toDeadLetter(DeadLetterEntry entry) {
        T eventMessage = (T) EventUtils.upcastAndDeserializeEvents(Stream.of(entry),
                                                                   serializer,
                                                                   upcasterChain)
                                       .findFirst()
                                       .orElse(null);

        return new GenericDeadLetter<>(
                entry.getDeadLetterId(),
                entry.getIdentifier(),
                eventMessage,
                entry.getCause(),
                entry.getDeadLetteredAt(),
                entry.getExpiresAt(),
                entry.getNumberOfRetries(),
                this::acknowledge,
                this::requeue
        );
    }

    private DeadLetterQueueIdentifier toJpaIdentifier(QueueIdentifier identifier) {
        return new DeadLetterQueueIdentifier(identifier.group(), identifier.identifier().toString());
    }

    private void acknowledge(DeadLetter<T> letter) {
        int removalResult = entityManager().createQuery(
                                                   "delete from DeadLetterEntry dl where dl.deadLetterId=:deadLetterId")
                                           .setParameter("deadLetterId", letter.identifier())
                                           .executeUpdate();
        if (removalResult == 0) {
            throw new DeadLetterEvaluationException("Could not remove letter with identifier " + letter.identifier()
                                                            + ". Was it removed from the database?");
        }
        entityManager().flush();
    }

    private void requeue(DeadLetter<T> letter) {
        EntityManager entityManager = entityManager();
        DeadLetterEntry letterEntity = entityManager.find(DeadLetterEntry.class, letter.identifier());
        Instant newExpiresAt = clock.instant().plus(expireThreshold);
        letterEntity.setExpiresAt(newExpiresAt);
        letterEntity.setNumberOfRetries(letterEntity.getNumberOfRetries() + 1);
        letterEntity.setProcessingStarted(null);
        entityManager.persist(letterEntity);
    }

    /**
     * Checks whether the queue contains a specified {@link QueueIdentifier}.
     * To improve performance, the JPA implementation keeps a map in the unitOfWork using already checked identifiers.
     * This prevents double queries to the database.
     *
     * @param identifier The identifier used to validate for contained {@link DeadLetter dead-letters} instances.
     */
    @Override
    public boolean contains(@Nonnull QueueIdentifier identifier) {
        if (logger.isDebugEnabled()) {
            logger.debug("Validating existence of sequence identifier [{}].", identifier.combinedIdentifier());
        }
        return dlqCache()
                .map(cache -> {
                    if (!cache.containsKey(identifier)) {
                        return cache.put(identifier, getMaxIndexForQueueIdentifier(identifier) != null);
                    }
                    return cache.get(identifier);
                })
                // No UnitOfWork active, fall back to simple query
                .orElseGet(() -> getMaxIndexForQueueIdentifier(identifier) != null);
    }

    private Optional<Map<QueueIdentifier, Boolean>> dlqCache() {
        return CurrentUnitOfWork.map(uow -> uow.getOrComputeResource("__dlq", s -> new HashMap<>()));
    }

    @Override
    public boolean isFull(@Nonnull QueueIdentifier queueIdentifier) {
        return maximumNumberOfQueuesReached(queueIdentifier) || maximumQueueSizeReached(queueIdentifier);
    }

    /**
     * Checks whether the maximum amount of unique {@link QueueIdentifier}s within this group of the DLQ is reached. If
     * there already is a dead letter with this {@link QueueIdentifier} present, it can be added to an already existing
     * queue and the method will return false.
     *
     * @param queueIdentifier The {@link QueueIdentifier} to check for.
     * @return Whether the maximum amount of queues is reached.
     */
    private boolean maximumNumberOfQueuesReached(QueueIdentifier queueIdentifier) {
        return getNumberInQueue(queueIdentifier) == 0 && getNumberOfQueues() >= maxQueues;
    }

    /**
     * Checks whether the maximum amount of dead letters within a {@link QueueIdentifier} is reached.
     *
     * @param queueIdentifier The {@link QueueIdentifier} to check for.
     * @return Whether the maximum amount of dead letters is reached.
     */
    private boolean maximumQueueSizeReached(QueueIdentifier queueIdentifier) {
        return getNumberInQueue(queueIdentifier) >= maxQueueSize;
    }

    /**
     * Counts the amount of messages for a given {@link QueueIdentifier}.
     *
     * @param queueIdentifier The {@link QueueIdentifier} to check for
     * @return The amount of dead letters for this {@link QueueIdentifier}
     */
    private Long getNumberInQueue(QueueIdentifier queueIdentifier) {
        EntityManager entityManager = entityManagerProvider.getEntityManager();
        return transactionManager.fetchInTransaction(
                () -> entityManager.createQuery(
                                           "SELECT count(dl) FROM DeadLetterEntry dl where dl.identifier.queueGroup=:group and dl.identifier.identifier=:identifier",
                                           Long.class
                                   )
                                   .setParameter("group", queueIdentifier.group())
                                   .setParameter("identifier", queueIdentifier.identifier())
                                   .getSingleResult()
        );
    }

    /**
     * Counts the amount of unique {@link QueueIdentifier}s within this DLQ.
     *
     * @return The amount of {@link QueueIdentifier}s
     */
    private Long getNumberOfQueues() {
        EntityManager entityManager = entityManagerProvider.getEntityManager();
        return transactionManager.fetchInTransaction(
                () -> entityManager.createQuery(
                                           "SELECT count(distinct dl.identifier) FROM DeadLetterEntry dl",
                                           Long.class
                                   )
                                   .getSingleResult()
        );
    }

    @Override
    public long maxQueues() {
        return maxQueues;
    }

    @Override
    public long maxQueueSize() {
        return maxQueueSize;
    }

    @Override
    public synchronized Optional<DeadLetter<T>> take(@Nonnull String group) {
        EntityManager entityManager = entityManagerProvider.getEntityManager();
        try {
            // Search for a dead letter. Has to belong to the group, have the lowest index for that QueueIdentifier and should be expired
            Instant processingStartedLimit = clock.instant().minus(expireThreshold);
            DeadLetterEntry entry = transactionManager.fetchInTransaction(
                    () -> entityManager.createQuery(
                                               "SELECT dl FROM DeadLetterEntry dl "
                                                       + "where dl.identifier.queueGroup=:group "
                                                       + "and dl.index = (select min(dl2.index) from DeadLetterEntry dl2 where dl2.identifier.queueGroup=dl.identifier.queueGroup and dl2.identifier=dl.identifier) "
                                                       + "and dl.expiresAt < :expiryLimit "
                                                       + "and (dl.processingStarted is null or dl.processingStarted < :processingStartedLimit) "
                                                       + "order by dl.expiresAt asc",
                                               DeadLetterEntry.class
                                       )
                                       .setParameter("group", group)
                                       .setParameter("expiryLimit", clock.instant())
                                       .setParameter("processingStartedLimit", processingStartedLimit)
                                       .setMaxResults(1)
                                       .getSingleResult()
            );

            // Claim the message in a separate transaction, so it's immediately visible to other nodes.
            transactionManager.executeInTransaction(() -> {
                int updatedRows = entityManager.createQuery(
                                                       "update DeadLetterEntry dl set dl.processingStarted=:time where dl.deadLetterId=:deadLetterId and (dl.processingStarted is null or dl.processingStarted < :processingStartedLimit)")
                                               .setParameter("deadLetterId", entry.getDeadLetterId())
                                               .setParameter("time", clock.instant())
                                               .setParameter("processingStartedLimit", processingStartedLimit)
                                               .executeUpdate();
                if (updatedRows == 0) {
                    throw new DeadLetterEvaluationException("Message with identifier " + entry.getIdentifier()
                                                                    + " was already claimed or processed by another node.");
                }
                entityManager().flush();
            });
            return Optional.of(toDeadLetter(entry));
        } catch (NoResultException e) {
            return Optional.empty();
        }
    }

    @Override
    public void release() {
        transactionManager.executeInTransaction(() -> {
            List<DeadLetterQueueIdentifier> releasedIdentifiers = getAllQueueIdentifiers()
                    .collect(Collectors.toList());

            entityManager().createQuery(
                                   "update DeadLetterEntry dl set dl.expiresAt=:time where dl.identifier in :identifiers")
                           .setParameter("time", clock.instant())
                           .setParameter("identifiers", releasedIdentifiers)
                           .executeUpdate();

            entityManager().flush();
            entityManager().clear();
            notifyAvailabilityCallbacksOfRelease(releasedIdentifiers);
        });
    }

    @Override
    public void release(@Nonnull String group) {
        transactionManager.executeInTransaction(() -> {
            List<DeadLetterQueueIdentifier> releasedIdentifiers = getAllQueueIdentifiers(group)
                    .collect(Collectors.toList());

            entityManager().createQuery(
                                   "update DeadLetterEntry dl set dl.expiresAt=:time")
                           .setParameter("time", clock.instant())
                           .executeUpdate();

            entityManager().flush();
            entityManager().clear();
            notifyAvailabilityCallbacksOfRelease(releasedIdentifiers);
        });
    }

    @Override
    public void release(@Nonnull Predicate<QueueIdentifier> queueFilter) {
        transactionManager.executeInTransaction(() -> {
            List<DeadLetterQueueIdentifier> releasedIdentifiers = getAllQueueIdentifiers()
                    .filter(queueFilter)
                    .collect(Collectors.toList());

            entityManager().createQuery(
                                   "update DeadLetterEntry dl set dl.expiresAt=:time where dl.identifier in :identifiers")
                           .setParameter("time", clock.instant())
                           .setParameter("identifiers", releasedIdentifiers)
                           .executeUpdate();

            entityManager().flush();
            entityManager().clear();
            notifyAvailabilityCallbacksOfRelease(releasedIdentifiers);
        });
    }

    private Stream<DeadLetterQueueIdentifier> getAllQueueIdentifiers() {
        return entityManager()
                .createQuery("select distinct(dl.identifier) from DeadLetterEntry dl",
                             DeadLetterQueueIdentifier.class)
                .getResultStream();
    }

    private Stream<DeadLetterQueueIdentifier> getAllQueueIdentifiers(String group) {
        return entityManager()
                .createQuery(
                        "select distinct(dl.identifier) from DeadLetterEntry dl where dl.identifier.queueGroup=:group",
                        DeadLetterQueueIdentifier.class)
                .setParameter("group", group)
                .getResultStream();
    }

    private void notifyAvailabilityCallbacksOfRelease(List<DeadLetterQueueIdentifier> releasedIdentifiers) {
        releasedIdentifiers.stream()
                           .map(DeadLetterQueueIdentifier::group)
                           .distinct()
                           .map(availabilityCallbacks::get)
                           .filter(Objects::nonNull)
                           .forEach(scheduledExecutorService::submit);
    }

    @Override
    public void clear(@Nonnull Predicate<QueueIdentifier> queueFilter) {

        List<QueueIdentifier> clearedIdentifiers = entityManager().createQuery(
                                                                          "select dl from DeadLetterEntry dl order by dl.expiresAt asc",
                                                                          DeadLetterEntry.class)
                                                                  .getResultStream()
                                                                  .map(this::toDeadLetter)
                                                                  .map(DeadLetter::queueIdentifier)
                                                                  .filter(queueFilter)
                                                                  .distinct()
                                                                  .collect(Collectors.toList());
        clearedIdentifiers.forEach(queueIdentifier ->
                                           entityManager()
                                                   .createQuery(
                                                           "delete from DeadLetterEntry dl where dl.identifier.identifier=:identifier and dl.identifier.queueGroup=:group")
                                                   .setParameter("group", queueIdentifier.group())
                                                   .setParameter("identifier", queueIdentifier.identifier())
                                                   .executeUpdate()
        );

        entityManager().flush();
    }


    /**
     * Provides an {@link EntityManager} instance for storing and fetching dead letter data.
     *
     * @return a provided entity manager
     */
    protected EntityManager entityManager() {
        return entityManagerProvider.getEntityManager();
    }


    /**
     * Builder class to instantiate an {@link JpaDeadLetterQueue}.
     * <p>
     * The maximum number of unique {@link QueueIdentifier}s defaults to {@code 1024}, the maximum amount of dead
     * letters inside an unique {@link QueueIdentifier} to {@code 1024}, the dead letter expire threshold defaults to a
     * {@link Duration} of 5000 milliseconds, and the {@link ScheduledExecutorService} defaults to a
     * {@link Executors#newSingleThreadScheduledExecutor(ThreadFactory)}, using an {@link AxonThreadFactory}.
     *
     * @param <T> The type of {@link Message} maintained in this {@link DeadLetterQueue}.
     */
    public static class Builder<T extends EventMessage<?>> extends SchedulingDeadLetterQueue.Builder<Builder<T>, T> {

        private int maxQueues = 1024;
        private int maxQueueSize = 1024;

        private EntityManagerProvider entityManagerProvider;
        private TransactionManager transactionManager;
        private Serializer serializer;
        protected EventUpcaster upcasterChain = NoOpEventUpcaster.INSTANCE;

        /**
         * Sets the maximum number of unique {@link QueueIdentifier}s this {@link DeadLetterQueue} may contain.
         * <p>
         * The given {@code maxQueues} is required to be a positive number, higher or equal to {@code 128}. It defaults
         * to {@code 1024}.
         *
         * @param maxQueues The maximum amount of unique {@link QueueIdentifier}s for the queue under construction.
         * @return The current Builder, for fluent interfacing.
         */
        public Builder<T> maxQueues(int maxQueues) {
            assertThat(maxQueues,
                       value -> value >= 128,
                       "The maximum number of queues should be larger or equal to 128");
            this.maxQueues = maxQueues;
            return this;
        }

        /**
         * Sets the maximum amount of {@link DeadLetter letters} per unique {@link QueueIdentifier} this
         * {@link DeadLetterQueue} can store.
         * <p>
         * The given {@code maxQueueSize} is required to be a positive number, higher or equal to {@code 128}. It
         * defaults to {@code 1024}.
         *
         * @param maxQueueSize The maximum amount of {@link DeadLetter letters} per unique {@link QueueIdentifier}.
         * @return The current Builder, for fluent interfacing.
         */
        public Builder<T> maxQueueSize(int maxQueueSize) {
            assertThat(maxQueueSize,
                       value -> value >= 128,
                       "The maximum number of entries in a queue should be larger or equal to 128");
            this.maxQueueSize = maxQueueSize;
            return this;
        }

        /**
         * Sets the {@link EntityManagerProvider} which provides the {@link EntityManager} used to access the underlying
         * database for this {@link JpaDeadLetterQueue} implementation.
         *
         * @param entityManagerProvider a {@link EntityManagerProvider} which provides the {@link EntityManager} used to
         *                              access the underlying database
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<T> entityManagerProvider(EntityManagerProvider entityManagerProvider) {
            assertNonNull(entityManagerProvider, "EntityManagerProvider may not be null");
            this.entityManagerProvider = entityManagerProvider;
            return this;
        }

        /**
         * Sets the {@link TransactionManager} used to manage transaction around fetching event data. Required by
         * certain databases for reading blob data.
         *
         * @param transactionManager a {@link TransactionManager} used to manage transaction around fetching event data
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<T> transactionManager(TransactionManager transactionManager) {
            assertNonNull(transactionManager, "TransactionManager may not be null");
            this.transactionManager = transactionManager;
            return this;
        }

        public Builder<T> eventSerializer(Serializer serializer) {
            assertNonNull(serializer, "The serializer may not be null");
            this.serializer = serializer;
            return this;
        }

        /**
         * Sets the {@link EventUpcaster} used to deserialize events of older revisions. Defaults to a
         * {@link NoOpEventUpcaster}.
         *
         * @param upcasterChain an {@link EventUpcaster} used to deserialize events of older revisions
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder<T> upcasterChain(EventUpcaster upcasterChain) {
            assertNonNull(upcasterChain, "EventUpcaster may not be null");
            this.upcasterChain = upcasterChain;
            return this;
        }

        /**
         * Initializes a {@link JpaDeadLetterQueue} as specified through this Builder.
         *
         * @return A {@link JpaDeadLetterQueue} as specified through this Builder.
         */
        public JpaDeadLetterQueue<T> build() {
            return new JpaDeadLetterQueue<>(this);
        }

        /**
         * Validate whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException When one field asserts to be incorrect according to the Builder's
         *                                    specifications.
         */
        @Override
        protected void validate() {
            super.validate();

            if (serializer == null) {
                logger.warn(
                        "The default XStreamSerializer is used for dead letters, whereas it is strongly recommended to "
                                + "configure the security context of the XStream instance."
                );
                serializer = XStreamSerializer.builder()
                                              .xStream(new XStream(new CompactDriver()))
                                              .build();
            }
        }
    }
}
