// launchd_activate: macOS socket-activation shim.
//
// Calls launch_activate_socket() for each requested launchd socket name
// and execs the given program with the systemd socket activation environment
// (LISTEN_PID, LISTEN_FDS) so that the program can use the same code path
// on Linux and macOS.
//
// Usage: launchd_activate [-n SOCKET_NAME]... -- PROGRAM [ARGS...]
//
// Socket names refer to keys of the Sockets dictionary in the launchd
// property list. If no -n is given, "Listeners" is used. File descriptors are
// placed in the order in which the names are provided.
//
// If the process was not started by launchd with a matching Sockets entry
// (launch_activate_socket returns ESRCH), the program is exec'ed with
// LISTEN_FDS=0 and no LISTEN_PID, so it falls back to regular listening.

#include <errno.h>
#include <fcntl.h>
#include <launch.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define SD_LISTEN_FDS_START 3
#define MAX_NAMES 16

static void die(const char *msg, int err) {
  fprintf(stderr, "launchd_activate: %s: %s\n", msg, strerror(err));
  exit(1);
}

int main(int argc, char **argv) {
  const char *names[MAX_NAMES];
  int nnames = 0;
  int i = 1;
  for (; i < argc; i++) {
    if (strcmp(argv[i], "--") == 0) { i++; break; }
    if (strcmp(argv[i], "-n") == 0 && i + 1 < argc && nnames < MAX_NAMES) {
      names[nnames++] = argv[++i];
    } else {
      break;
    }
  }
  if (i >= argc) {
    fprintf(stderr, "usage: %s [-n SOCKET_NAME]... -- PROGRAM [ARGS...]\n", argv[0]);
    return 2;
  }
  char **prog_argv = &argv[i];
  if (nnames == 0) names[nnames++] = "Listeners";

  // Never trust inherited values; they would refer to someone else's FDs.
  unsetenv("LISTEN_PID");
  unsetenv("LISTEN_FDS");

  // Programs expect the sockets at file descriptors 3, 4, ... Rather than
  // rearranging file descriptors, verify that launchd gave them to us in that
  // order.
  size_t total = 0;
  for (int n = 0; n < nnames; n++) {
    int *fds = NULL;
    size_t cnt = 0;
    int rc = launch_activate_socket(names[n], &fds, &cnt);
    if (rc == ESRCH || rc == ENOENT) continue;  // not activated / no such socket
    if (rc != 0) die("launch_activate_socket", rc);
    for (size_t k = 0; k < cnt; k++, total++) {
      int expected = SD_LISTEN_FDS_START + (int)total;
      if (fds[k] != expected) {
        fprintf(stderr,
                "launchd_activate: socket %zu has file descriptor %d, "
                "expected %d\n",
                total, fds[k], expected);
        return 1;
      }
      // The sockets need to survive exec().
      if (fcntl(fds[k], F_SETFD, 0) < 0) die("fcntl(F_SETFD)", errno);
    }
    free(fds);
  }

  // Always export LISTEN_FDS, so that configuration files can refer to it
  // unconditionally through std.extVar("LISTEN_FDS"). Zero means that no
  // sockets were passed down.
  char buf[32];
  snprintf(buf, sizeof buf, "%zu", total);
  setenv("LISTEN_FDS", buf, 1);
  if (total > 0) {
    snprintf(buf, sizeof buf, "%d", getpid());  // exec keeps the PID
    setenv("LISTEN_PID", buf, 1);
  }

  execvp(prog_argv[0], prog_argv);
  die(prog_argv[0], errno);
  return 1;
}
