/*
 * Copyright (c) 2010-2020. Axon Framework
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

package org.axonframework.messaging.annotation;

import org.axonframework.messaging.Message;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.Collections.emptySortedSet;

/**
 * Inspector for a message handling target of type {@code T} that uses annotations on the target to inspect the
 * capabilities of the target.
 *
 * @param <T> the target type
 */
public class AnnotatedHandlerInspector<T> {

    private final Class<T> inspectedType;
    private final ParameterResolverFactory parameterResolverFactory;
    private final Map<Class<?>, AnnotatedHandlerInspector<?>> registry;
    private final List<AnnotatedHandlerInspector<? super T>> superClassInspectors;
    private final List<AnnotatedHandlerInspector<? extends T>> subClassInspectors;
    private final Map<Class<?>, SortedSet<MessageHandlingMember<? super T>>> handlers;
    private final HandlerDefinition handlerDefinition;
    private final Map<Class<?>, MessageHandlerInterceptorMemberChain<T>> interceptorChains;
    private final Map<Class<?>, SortedSet<MessageHandlingMember<? super T>>> interceptors;

    private AnnotatedHandlerInspector(Class<T> inspectedType,
                                      List<AnnotatedHandlerInspector<? super T>> superClassInspectors,
                                      ParameterResolverFactory parameterResolverFactory,
                                      HandlerDefinition handlerDefinition,
                                      Map<Class<?>, AnnotatedHandlerInspector<?>> registry,
                                      List<AnnotatedHandlerInspector<? extends T>> subClassInspectors) {
        this.inspectedType = inspectedType;
        this.parameterResolverFactory = parameterResolverFactory;
        this.registry = registry;
        this.superClassInspectors = new ArrayList<>(superClassInspectors);
        this.handlers = new HashMap<>();
        this.handlerDefinition = handlerDefinition;
        this.subClassInspectors = subClassInspectors;
        this.interceptorChains = new ConcurrentHashMap<>();
        this.interceptors = new ConcurrentHashMap<>();
    }

    /**
     * Create an inspector for given {@code handlerType} that uses a {@link ClasspathParameterResolverFactory} to
     * resolve method parameters and {@link ClasspathHandlerDefinition} to create handlers.
     *
     * @param handlerType the target handler type
     * @param <T>         the handler's type
     * @return a new inspector instance for the inspected class
     */
    public static <T> AnnotatedHandlerInspector<T> inspectType(Class<? extends T> handlerType) {
        return inspectType(handlerType, ClasspathParameterResolverFactory.forClass(handlerType));
    }

    /**
     * Create an inspector for given {@code handlerType} that uses given {@code parameterResolverFactory} to resolve
     * method parameters.
     *
     * @param handlerType              the target handler type
     * @param parameterResolverFactory the resolver factory to use during detection
     * @param <T>                      the handler's type
     * @return a new inspector instance for the inspected class
     */
    public static <T> AnnotatedHandlerInspector<T> inspectType(Class<? extends T> handlerType,
                                                               ParameterResolverFactory parameterResolverFactory) {
        return inspectType(handlerType,
                           parameterResolverFactory,
                           ClasspathHandlerDefinition.forClass(handlerType));
    }

    /**
     * Create an inspector for given {@code handlerType} that uses given {@code parameterResolverFactory} to resolve
     * method parameters and given {@code handlerDefinition} to create handlers.
     *
     * @param handlerType              the target handler type
     * @param parameterResolverFactory the resolver factory to use during detection
     * @param handlerDefinition        the handler definition used to create concrete handlers
     * @param <T>                      the handler's type
     * @return a new inspector instance for the inspected class
     */
    public static <T> AnnotatedHandlerInspector<T> inspectType(Class<? extends T> handlerType,
                                                               ParameterResolverFactory parameterResolverFactory,
                                                               HandlerDefinition handlerDefinition) {
        return inspectType(handlerType, parameterResolverFactory, handlerDefinition, emptySet());
    }

    /**
     * Create an inspector for given {@code handlerType} and its {@code declaredSubtypes} that uses given {@code
     * parameterResolverFactory} to resolve method parameters and given {@code handlerDefinition} to create handlers.
     *
     * @param handlerType              the target handler type
     * @param parameterResolverFactory the resolver factory to use during detection
     * @param handlerDefinition        the handler definition used to create concrete handlers
     * @param declaredSubtypes         the declared subtypes of this {@code handlerType}
     * @param <T>                      the handler's type
     * @return a new inspector instance for the inspected class
     */
    public static <T> AnnotatedHandlerInspector<T> inspectType(Class<? extends T> handlerType,
                                                               ParameterResolverFactory parameterResolverFactory,
                                                               HandlerDefinition handlerDefinition,
                                                               Set<Class<? extends T>> declaredSubtypes) {
        return createInspector(handlerType,
                               parameterResolverFactory,
                               handlerDefinition,
                               new HashMap<>(),
                               declaredSubtypes);
    }

    @SuppressWarnings("unchecked")
    private static <T> AnnotatedHandlerInspector<T> createInspector(Class<? extends T> inspectedType,
                                                                    ParameterResolverFactory parameterResolverFactory,
                                                                    HandlerDefinition handlerDefinition,
                                                                    Map<Class<?>, AnnotatedHandlerInspector<?>> registry,
                                                                    Set<Class<? extends T>> declaredSubtypes) {
        if (!registry.containsKey(inspectedType)) {
            registry.put(inspectedType,
                         AnnotatedHandlerInspector.initialize((Class<T>) inspectedType,
                                                              parameterResolverFactory,
                                                              handlerDefinition,
                                                              registry,
                                                              declaredSubtypes));
        }
        //noinspection unchecked
        return (AnnotatedHandlerInspector<T>) registry.get(inspectedType);
    }

    private static <T> AnnotatedHandlerInspector<T> initialize(Class<T> inspectedType,
                                                               ParameterResolverFactory parameterResolverFactory,
                                                               HandlerDefinition handlerDefinition,
                                                               Map<Class<?>, AnnotatedHandlerInspector<?>> registry,
                                                               Set<Class<? extends T>> declaredSubtypes) {
        List<AnnotatedHandlerInspector<? super T>> parents = new ArrayList<>();
        for (Class<?> iFace : inspectedType.getInterfaces()) {
            parents.add(createInspector(iFace,
                                        parameterResolverFactory,
                                        handlerDefinition,
                                        registry,
                                        emptySet()));
        }
        if (inspectedType.getSuperclass() != null && !Object.class.equals(inspectedType.getSuperclass())) {
            parents.add(createInspector(inspectedType.getSuperclass(),
                                        parameterResolverFactory,
                                        handlerDefinition,
                                        registry,
                                        emptySet()));
        }
        List<AnnotatedHandlerInspector<? extends T>> children =
                declaredSubtypes.stream()
                                .map(subclass -> createInspector(subclass,
                                                                 parameterResolverFactory,
                                                                 handlerDefinition,
                                                                 registry,
                                                                 emptySet()))
                                .collect(Collectors.toList());
        AnnotatedHandlerInspector<T> inspector = new AnnotatedHandlerInspector<T>(inspectedType,
                                                                                  parents,
                                                                                  parameterResolverFactory,
                                                                                  handlerDefinition,
                                                                                  registry,
                                                                                  children);
        inspector.initializeMessageHandlers(parameterResolverFactory, handlerDefinition);
        return inspector;
    }

    @SuppressWarnings("unchecked")
    private void initializeMessageHandlers(ParameterResolverFactory parameterResolverFactory,
                                           HandlerDefinition handlerDefinition) {
        handlers.put(inspectedType, new TreeSet<>(HandlerComparator.instance()));
        for (Method method : inspectedType.getDeclaredMethods()) {
            handlerDefinition.createHandler(inspectedType, method, parameterResolverFactory)
                             .ifPresent(h -> registerHandler(inspectedType, h));
        }
        for (Constructor<?> constructor : inspectedType.getDeclaredConstructors()) {
            handlerDefinition.createHandler(inspectedType, constructor, parameterResolverFactory)
                             .ifPresent(h -> registerHandler(inspectedType, h));
        }

        // we need to consider handlers from parent/subclasses as well
        subClassInspectors.forEach(sci -> sci.getAllHandlers()
                                             .forEach((key, value) -> value.forEach(h -> registerHandler(key, (MessageHandlingMember<T>) h))));
        superClassInspectors.forEach(sci -> sci.getAllHandlers()
                                               .forEach((key, value) -> value.forEach(h -> {
                                                   registerHandler(key, h);
                                                   registerHandler(inspectedType, h);
                                               })));

        // we need to consider interceptors from parent/subclasses as well
        subClassInspectors.forEach(sci -> sci.getAllInterceptors()
                                             .forEach((key, value) -> value.forEach(h -> registerHandler(key, (MessageHandlingMember<T>) h))));
        superClassInspectors.forEach(sci -> sci.getAllInterceptors()
                                               .forEach((key, value) -> value.forEach(h -> {
                                                   registerHandler(key, h);
                                                   registerHandler(inspectedType, h);
                                               })));
    }

    private void registerHandler(Class<?> type, MessageHandlingMember<? super T> handler) {
        if (handler.unwrap(MessageHandlerInterceptorMember.class).isPresent()) {
            interceptors.computeIfAbsent(type, t -> new TreeSet<>(HandlerComparator.instance()))
                        .add(handler);

        } else {
            handlers.computeIfAbsent(type, t -> new TreeSet<>(HandlerComparator.instance()))
                    .add(handler);
        }
    }

    /**
     * Inspect another handler type and register the result to the inspector registry of this inspector. This is used
     * by Axon to inspect child entities of an aggregate.
     *
     * @param entityType the type of the handler to inspect
     * @param <C>        the handler's type
     *
     * @return a new inspector for the given type
     */
    public <C> AnnotatedHandlerInspector<C> inspect(Class<? extends C> entityType) {
        return AnnotatedHandlerInspector.createInspector(entityType,
                                                         parameterResolverFactory,
                                                         handlerDefinition,
                                                         registry,
                                                         emptySet());
    }

    /**
     * Returns a list of detected members of the inspected entity that are capable of handling certain messages.
     *
     * @return a list of detected message handlers
     * @deprecated use {@link #getAllHandlers()} or {@link #getHandlers(Class)} instead
     */
    @Deprecated
    public List<MessageHandlingMember<? super T>> getHandlers() {
        return getHandlers(inspectedType).collect(Collectors.toList());
    }

    /**
     * Returns a list of detected members of given {@code type} that are capable of handling certain messages.
     *
     * @param type a type of inspected entity
     *
     * @return a stream of detected message handlers for given {@code type}
     */
    public Stream<MessageHandlingMember<? super T>> getHandlers(Class<?> type) {
        return handlers.getOrDefault(type, emptySortedSet())
                       .stream();
    }

    /**
     * Returns an Interceptor Chain of annotated interceptor methods defined on the given
     * {@code type}. The given chain will invoke all relevant interceptors in an order defined
     * by the handler definition.
     *
     * @param type The type containing the handler definitions
     *
     * @return an interceptor chain that invokes the interceptor handlers defined on the inspected type
     */
    public MessageHandlerInterceptorMemberChain<T> chainedInterceptor(Class<?> type) {
        return interceptorChains.computeIfAbsent(type, t -> {
            Collection<MessageHandlingMember<? super T>> i = interceptors.getOrDefault(type, emptySortedSet());
            if (i.isEmpty()) {
                return NoMoreInterceptors.instance();
            }
            return new ChainedMessageHandlerInterceptorMember<>(t, i.iterator());
        });
    }

    /**
     * Gets all handlers per type for inspected entity. Handlers are sorted based on {@link HandlerComparator}.
     *
     * @return a map of handlers per type
     */
    public Map<Class<?>, SortedSet<MessageHandlingMember<? super T>>> getAllHandlers() {
        return Collections.unmodifiableMap(handlers);
    }

    /**
     * Returns a Map of all registered interceptor methods per inspected type. Each entry
     * contains the inspected type as key, and a SortedSet of interceptor methods defined
     * on that type, in the order they are considered for invocation.
     *
     * @return a map of interceptors per type
     */
    public Map<Class<?>, SortedSet<MessageHandlingMember<? super T>>> getAllInterceptors() {
        return Collections.unmodifiableMap(interceptors);
    }

    private static class ChainedMessageHandlerInterceptorMember<T> implements MessageHandlerInterceptorMemberChain<T> {
        private final MessageHandlingMember<? super T> delegate;
        private final MessageHandlerInterceptorMemberChain<T> next;

        private ChainedMessageHandlerInterceptorMember(Class<?> targetType, Iterator<MessageHandlingMember<? super T>> iterator) {
            this.delegate = iterator.next();
            if (iterator.hasNext()) {
                this.next = new ChainedMessageHandlerInterceptorMember<>(targetType, iterator);
            } else {
                this.next = NoMoreInterceptors.instance();
            }
        }

        @Override
        public Object handle(Message<?> message, T target, MessageHandlingMember<? super T> handler) throws Exception {
            return InterceptorChainParameterResolverFactory.callWithInterceptorChain(() -> next.handle(message, target, handler),
                                                                                     () -> doHandle(message, target, handler));
        }

        private Object doHandle(Message<?> message, T target, MessageHandlingMember<? super T> handler) throws Exception {
            if (delegate.canHandle(message)) {
                return delegate.handle(message, target);
            }
            return next.handle(message, target, handler);
        }
    }

    private static class NoMoreInterceptors<T> implements MessageHandlerInterceptorMemberChain<T> {

        static <T> MessageHandlerInterceptorMemberChain<T> instance() {
            return new NoMoreInterceptors<>();
        }

        @Override
        public Object handle(Message<?> message, T target, MessageHandlingMember<? super T> handler) throws Exception {
            return handler.handle(message, target);
        }
    }
}
