/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class SeedHostsResolver extends AbstractLifecycleComponent implements ConfiguredHostsResolver, SeedHostsProvider.HostsResolver {
    public static final Setting<Integer> DISCOVERY_SEED_RESOLVER_MAX_CONCURRENT_RESOLVERS_SETTING = Setting.intSetting(
        "discovery.seed_resolver.max_concurrent_resolvers",
        10,
        0,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING = Setting.positiveTimeSetting(
        "discovery.seed_resolver.timeout",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(SeedHostsResolver.class);

    private final Settings settings;
    private final AtomicBoolean resolveInProgress = new AtomicBoolean();
    private final TransportService transportService;
    private final SeedHostsProvider hostsProvider;
    private final SetOnce<ExecutorService> executorService = new SetOnce<>();
    private final TimeValue resolveTimeout;
    private final String nodeName;
    private final int concurrentConnects;
    private final CancellableThreads cancellableThreads = new CancellableThreads();

    public SeedHostsResolver(String nodeName, Settings settings, TransportService transportService, SeedHostsProvider seedProvider) {
        this.settings = settings;
        this.nodeName = nodeName;
        this.transportService = transportService;
        this.hostsProvider = seedProvider;
        resolveTimeout = getResolveTimeout(settings);
        concurrentConnects = getMaxConcurrentResolvers(settings);
    }

    public static int getMaxConcurrentResolvers(Settings settings) {
        return DISCOVERY_SEED_RESOLVER_MAX_CONCURRENT_RESOLVERS_SETTING.get(settings);
    }

    public static TimeValue getResolveTimeout(Settings settings) {
        return DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING.get(settings);
    }

    @Override
    public List<TransportAddress> resolveHosts(final List<String> hosts) {
        Objects.requireNonNull(hosts);
        if (resolveTimeout.nanos() < 0) {
            throw new IllegalArgumentException("resolve timeout must be non-negative but was [" + resolveTimeout + "]");
        }

        final long startTimeNanos = transportService.getThreadPool().relativeTimeInNanos();
        final List<TransportAddress> transportAddresses = new ArrayList<>(hosts.size());
        final CancellableThreads localCancellableThreads = new CancellableThreads();
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        try (var refs = new RefCountingRunnable(() -> future.onResponse(null))) {
            for (final var host : hosts) {
                executorService.get().execute(ActionRunnable.run(refs.acquireListener(), () -> localCancellableThreads.execute(() -> {
                    final List<TransportAddress> hostAddresses;
                    try {
                        hostAddresses = Arrays.asList(transportService.addressesFromString(host));
                    } catch (UnknownHostException e) {
                        logger.warn("failed to resolve host [" + host + "]", e.getCause());
                        return;
                    }
                    if (localCancellableThreads.isCancelled()) {
                        var duration = TimeValue.timeValueNanos(transportService.getThreadPool().relativeTimeInNanos() - startTimeNanos);
                        logger.warn(
                            "timed out after [{}/{}ms] ([{}]=[{}]) resolving host [{}]",
                            duration,
                            duration.getMillis(),
                            DISCOVERY_SEED_RESOLVER_TIMEOUT_SETTING.getKey(),
                            resolveTimeout,
                            host
                        );
                    } else if (hostAddresses.size() > 0) {
                        synchronized (transportAddresses) {
                            transportAddresses.addAll(hostAddresses);
                        }
                    }
                })));
            }
        }

        final var timeout = transportService.getThreadPool()
            .schedule(
                () -> localCancellableThreads.cancel(Strings.format("timed out after [%s/%dms]", resolveTimeout, resolveTimeout.millis())),
                resolveTimeout,
                ThreadPool.Names.SAME
            );

        try {
            cancellableThreads.execute(() -> {
                try {
                    localCancellableThreads.getUninterruptibly(future);
                } catch (ExecutionException e) {
                    assert false : e;
                    throw new UncategorizedExecutionException("unexpected failure", e);
                }
            });
        } catch (CancellableThreads.ExecutionCancelledException e) {
            return List.of();
        } finally {
            timeout.cancel();
        }

        return Collections.unmodifiableList(transportAddresses);
    }

    @Override
    protected void doStart() {
        logger.debug("using max_concurrent_resolvers [{}], resolver timeout [{}]", concurrentConnects, resolveTimeout);
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings, "[unicast_configured_hosts_resolver]");
        executorService.set(
            EsExecutors.newScaling(
                nodeName + "/unicast_configured_hosts_resolver",
                0,
                concurrentConnects,
                60,
                TimeUnit.SECONDS,
                false,
                threadFactory,
                transportService.getThreadPool().getThreadContext()
            )
        );
    }

    @Override
    protected void doStop() {
        cancellableThreads.cancel("stopping SeedHostsResolver");
        ThreadPool.terminate(executorService.get(), 10, TimeUnit.SECONDS);
    }

    @Override
    protected void doClose() {}

    @Override
    public void resolveConfiguredHosts(Consumer<List<TransportAddress>> consumer) {
        if (lifecycle.started() == false) {
            logger.debug("resolveConfiguredHosts: lifecycle is {}, not proceeding", lifecycle);
            return;
        }

        if (resolveInProgress.compareAndSet(false, true)) {
            transportService.getThreadPool().executor(ThreadPool.Names.CLUSTER_COORDINATION).execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.debug("failure when resolving unicast hosts list", e);
                }

                @Override
                protected void doRun() {
                    if (lifecycle.started() == false) {
                        logger.debug("resolveConfiguredHosts.doRun: lifecycle is {}, not proceeding", lifecycle);
                        return;
                    }

                    List<TransportAddress> providedAddresses = hostsProvider.getSeedAddresses(SeedHostsResolver.this);

                    consumer.accept(providedAddresses);
                }

                @Override
                public void onAfter() {
                    resolveInProgress.set(false);
                }

                @Override
                public String toString() {
                    return "SeedHostsResolver resolving unicast hosts list";
                }
            });
        }
    }
}
