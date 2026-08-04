#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <unistd.h>

#define MAX_EVENTS 10
#define BUFFER_SIZE 256

// Helper function to set file descriptors to non-blocking mode
int set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1)
    return -1;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Callback handler for input events
void handle_stdin_read(int fd) {
  char buf[BUFFER_SIZE];
  ssize_t bytes_read = read(fd, buf, sizeof(buf) - 1);

  if (bytes_read > 0) {
    buf[bytes_read] = '\0';
    printf("[Event Loop] Received input (%zd bytes): %s", bytes_read, buf);
  } else if (bytes_read == 0) {
    printf("[Event Loop] EOF reached.\n");
  } else {
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      perror("read error");
    }
  }
}

int main() {
  // 1. Create the epoll instance
  int epoll_fd = epoll_create1(0);
  if (epoll_fd == -1) {
    perror("epoll_create1 failed");
    exit(EXIT_FAILURE);
  }

  // 2. Set stdin to non-blocking mode
  if (set_nonblocking(STDIN_FILENO) == -1) {
    perror("set_nonblocking failed");
    close(epoll_fd);
    exit(EXIT_FAILURE);
  }

  // 3. Register STDIN (fd 0) with epoll
  struct epoll_event ev;
  ev.events = EPOLLIN; // Watch for readable data
  ev.data.fd = STDIN_FILENO;

  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, STDIN_FILENO, &ev) == -1) {
    perror("epoll_ctl: STDIN_FILENO");
    close(epoll_fd);
    exit(EXIT_FAILURE);
  }

  struct epoll_event events[MAX_EVENTS];
  int running = 1;

  printf("Single-threaded event loop running. Type something and press Enter "
         "(or Ctrl+C to quit):\n\n");

  // 4. Main Event Loop
  while (running) {
    // Wait up to 2000ms (2 seconds) for events
    int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, 2000);

    if (nfds == -1) {
      if (errno == EINTR)
        continue; // Interrupted by system signal, retry
      perror("epoll_wait error");
      break;
    }

    if (nfds == 0) {
      // Idle timeout handling
      printf("[Event Loop] Idle tick (no I/O events in last 2 seconds)...\n");
      continue;
    }

    // Dispatch triggered events sequentially
    for (int i = 0; i < nfds; i++) {
      int fd = events[i].data.fd;

      if (events[i].events & EPOLLIN) {
        if (fd == STDIN_FILENO) {
          handle_stdin_read(fd);
        }
      }

      if (events[i].events & (EPOLLERR | EPOLLHUP)) {
        fprintf(stderr, "[Event Loop] Error on fd %d\n", fd);
        close(fd);
      }
    }
  }

  close(epoll_fd);
  return 0;
}