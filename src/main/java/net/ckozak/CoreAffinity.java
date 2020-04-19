package net.ckozak;

final class CoreAffinity {
    private static final String ALL_CORES = "0xffffffff";

    static void useCores(int requestedCores) {
        int available = Runtime.getRuntime().availableProcessors();
        if (requestedCores > available) {
            throw new IllegalArgumentException(
                    "Requested " + requestedCores + " cores, but only " + available + " are available");
        }
        try {
            long pid = ProcessHandle.current().pid();
            Process process = new ProcessBuilder()
                    .inheritIO()
                    .command("taskset", "-a", "-p", toMask(requestedCores), Long.toString(pid))
                    .start();
            if (process.waitFor() != 0) {
                throw new RuntimeException("bad exit code");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String toMask(int cores) {
        if (cores <= 0) {
            return ALL_CORES;
        }
        int mask = 0;
        for (int i = 0; i < cores; i++) {
            mask |= 1 << i;
        }
        return Integer.toHexString(mask);
    }
}
