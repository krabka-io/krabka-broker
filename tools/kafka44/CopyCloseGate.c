/* Test-only process-scoped gate: real archive payload close, before fs::copy returns.
 * Compile cc -shared -fPIC -o gate.so CopyCloseGate.c -ldl.
 * COPY_GATE_ROOT must be an absolute owned archive path ending with '/'.
 * COPY_GATE_MARKER is an owned marker; remove COPY_GATE_ROOT on recovery.
 * No bytes are fabricated; this does not emulate power loss or partial writes.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdatomic.h>
static atomic_flag gated = ATOMIC_FLAG_INIT;
int close(int fd) {
    int (*real_close)(int) = dlsym(RTLD_NEXT, "close");
    const char *root = getenv("COPY_GATE_ROOT");
    const char *marker = getenv("COPY_GATE_MARKER");
    char link[64], path[PATH_MAX];
    int selected = 0;
    if (root && marker && root[0] == '/' && root[strlen(root)-1] == '/') {
        snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
        ssize_t n = readlink(link, path, sizeof(path)-1);
        if (n > 4) {
            path[n] = 0;
            selected = strncmp(path, root, strlen(root)) == 0 && strcmp(path+n-4, ".log") == 0;
        }
    }
    int result = real_close(fd);
    if (result == 0 && selected && !atomic_flag_test_and_set(&gated)) {
        int witness = open(marker, O_WRONLY|O_CREAT|O_EXCL, 0600);
        if (witness < 0) _exit(91);
        if (write(witness, path, strlen(path)) != (ssize_t)strlen(path)) _exit(92);
        if (fsync(witness) != 0) _exit(93);
        real_close(witness);
        for (;;) pause();
    }
    return result;
}
