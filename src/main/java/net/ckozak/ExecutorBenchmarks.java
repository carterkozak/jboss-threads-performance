package net.ckozak;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.threads.EnhancedQueueExecutor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@org.openjdk.jmh.annotations.State(Scope.Benchmark)
@Measurement(iterations = 4, time = 7)
@Warmup(iterations = 2, time = 7)
// Use a consistent heap and GC configuration to avoid confusion between JVMS with differing GC defaults.
@Fork(
        value = 1,
        jvmArgs = {"-Xmx2g", "-Xms2g", "-XX:+UseParallelOldGC", "-XX:-UseBiasedLocking"})
public class ExecutorBenchmarks {

    // this can be used to avoid the near-empty syndrome
    static final long DELAY_CONSUMER = Long.getLong("delay.c", 0L);

    @Param({"LTQ_THREAD_POOL_EXECUTOR", "LBQ_THREAD_POOL_EXECUTOR", "EQE_NO_LOCKS", "EQE_SPIN_LOCK"})
    public ExecutorType factory;

    @Param ({ "1", "2", "4", "8", "14", "28" })
    public int cores;

    @Param ({ "16,16", "100,200" })
    public String executorThreads;

    @Param( {"1", "10", "100"})
    public int burstSize;

    public enum ExecutorType {
        LBQ_THREAD_POOL_EXECUTOR(LockMode.NO_LOCKS /* any value */),
        LTQ_THREAD_POOL_EXECUTOR(LockMode.NO_LOCKS /* any value */),
        EQE_NO_LOCKS(LockMode.NO_LOCKS),
        EQE_SPIN_LOCK(LockMode.SPIN_LOCK);

        private final LockMode mode;

        ExecutorType(LockMode mode) {
            this.mode = mode;
        }

        ExecutorService create(String executorThreads) {
            mode.install();
            String[] coreAndMax = executorThreads.split(",");
            if (coreAndMax.length != 2) {
                throw new IllegalStateException("Failed to parse " + executorThreads);
            }
            int coreThreads = Integer.parseInt(coreAndMax[0].trim());
            int maxThreads = Integer.parseInt(coreAndMax[1].trim());
            if (this == LBQ_THREAD_POOL_EXECUTOR) {
                return new ThreadPoolExecutor(coreThreads, maxThreads,
                        1L, TimeUnit.MINUTES,
                        new LinkedBlockingQueue<>(),
                        Executors.defaultThreadFactory());
            } else if (this == LTQ_THREAD_POOL_EXECUTOR) {
                return new ThreadPoolExecutor(coreThreads, maxThreads,
                    1L, TimeUnit.MINUTES,
                    new LinkedTransferQueue<>(),
                    Executors.defaultThreadFactory());
            }
            return new EnhancedQueueExecutor.Builder()
                    .setCorePoolSize(coreThreads)
                    .setMaximumPoolSize(maxThreads)
                    .setKeepAliveTime(Duration.ofMinutes(1))
                    .setThreadFactory(Executors.defaultThreadFactory())
                    .build();
        }
    }

    public enum LockMode {
        NO_LOCKS,
        SPIN_LOCK,
        SYNCHRONIZED,
        REENTRANT_LOCK;

        void install() {
            switch (this) {
                case NO_LOCKS:
                    System.setProperty("jboss.threads.eqe.tail-lock", "false");
                    System.setProperty("jboss.threads.eqe.head-lock", "false");
                    break;
                case SPIN_LOCK:
                    // default
                    break;
                default:
                    throw new IllegalStateException("Unknown state: " + this);
            }
        }
    }
    private ExecutorService executor;

    @Setup
    public void setup() {
        CoreAffinity.useCores(cores);
        executor = factory.create(executorThreads);
    }

    @TearDown
    public void tearDown() throws InterruptedException {
        executor.shutdownNow();
        if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
            throw new IllegalStateException("executor failed to teminate within 1 second");
        }
    }

    @State(Scope.Thread)
    public static class ExecutingTasks implements Runnable {

        Blackhole blackhole;
        Future<?>[] pendingTasks;
        int burstSize;

        @Setup(Level.Iteration)
        public void init(Blackhole blackhole, ExecutorBenchmarks benchmarks) {
            burstSize = benchmarks.burstSize;
            pendingTasks = new FutureTask<?>[benchmarks.burstSize];
            this.blackhole = blackhole;
        }

        @Override
        public void run()
        {
            if (DELAY_CONSUMER > 0) {
                Blackhole.consumeCPU(DELAY_CONSUMER);
            }
        }

        public void submitBurst(ExecutorService executor)
        {
            Future<?>[] pendingTasks = this.pendingTasks;
            for (int i = 0, size = burstSize; i < size; i++)
            {
                pendingTasks[i] = executor.submit(this);
            }
        }

        public void awaitBurstCompletion() throws ExecutionException, InterruptedException
        {
            Future<?>[] pendingTasks = this.pendingTasks;
            for (int i = 0, size = burstSize; i < size; i++) {
                int index = size - (i+1);
                Future<?> lastTask = pendingTasks[index];
                pendingTasks[index] = null;
                lastTask.get();
            }
        }
    }

    @Threads(32)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void benchmarkSubmit(ExecutingTasks executingTasks) throws Exception {
        executingTasks.submitBurst(executor);
        executingTasks.awaitBurstCompletion();
    }

    public static void main(String[] args) throws RunnerException {
        new Runner(new OptionsBuilder()
                .include(ExecutorBenchmarks.class.getSimpleName())
                .build())
                .run();
    }
}
