#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#define PORT 8080
#define MAX_EVENTS 64
#define BUFFER_SIZE 1024

// Set file descriptor to non-blocking mode
int set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1)
    return -1;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Create and bind non-blocking IPv4 TCP server socket
int create_server_socket(int port) {
  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd == -1) {
    perror("socket failed");
    return -1;
  }

  // Allow quick reuse of the port after restart
  int opt = 1;
  if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    perror("setsockopt failed");
    close(listen_fd);
    return -1;
  }

  struct sockaddr_in address;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);

  if (bind(listen_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
    perror("bind failed");
    close(listen_fd);
    return -1;
  }

  if (listen(listen_fd, SOMAXCONN) < 0) {
    perror("listen failed");
    close(listen_fd);
    return -1;
  }

  if (set_nonblocking(listen_fd) < 0) {
    perror("set_nonblocking failed");
    close(listen_fd);
    return -1;
  }

  return listen_fd;
}

// Accept incoming client connections
void handle_accept(int epoll_fd, int listen_fd) {
  while (1) {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    // Accept incoming connection
    int conn_fd =
        accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);
    if (conn_fd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // All pending connections have been processed
        break;
      } else {
        perror("accept error");
        break;
      }
    }

    if (set_nonblocking(conn_fd) < 0) {
      perror("set_nonblocking client failed");
      close(conn_fd);
      continue;
    }

    // Register client socket for reading with Edge-Triggered mode (EPOLLET)
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
    ev.data.fd = conn_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_fd, &ev) < 0) {
      perror("epoll_ctl: conn_fd");
      close(conn_fd);
    } else {
      printf("[Server] New client connected on fd %d\n", conn_fd);
    }
  }
}

// Read data from client and echo it back
void handle_client_data(int epoll_fd, int conn_fd) {
  char buffer[BUFFER_SIZE];

  // Read all available data in non-blocking mode
  while (1) {
    ssize_t bytes_read = read(conn_fd, buffer, sizeof(buffer));

    if (bytes_read > 0) {
      // Echo data back to client
      ssize_t bytes_written = write(conn_fd, buffer, bytes_read);
      if (bytes_written < 0 && (errno != EAGAIN && errno != EWOULDBLOCK)) {
        perror("write error");
        close(conn_fd);
        break;
      }
    } else if (bytes_read == 0) {
      // Connection closed by client
      printf("[Server] Client on fd %d disconnected\n", conn_fd);
      epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn_fd, NULL);
      close(conn_fd);
      break;
    } else {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Read buffer exhausted for now
        break;
      }
      perror("read error");
      epoll_ctl(epoll_fd, EPOLL_CTL_DEL, conn_fd, NULL);
      close(conn_fd);
      break;
    }
  }
}

int main() {
  int listen_fd = create_server_socket(PORT);
  if (listen_fd < 0)
    exit(EXIT_FAILURE);

  int epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    perror("epoll_create1 failed");
    close(listen_fd);
    exit(EXIT_FAILURE);
  }

  // Register listening socket with epoll
  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET; // Edge-triggered
  ev.data.fd = listen_fd;

  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) {
    perror("epoll_ctl: listen_fd");
    close(listen_fd);
    close(epoll_fd);
    exit(EXIT_FAILURE);
  }

  struct epoll_event events[MAX_EVENTS];
  printf("[Server] Listening on port %d (Single-threaded event loop)...\n",
         PORT);

  while (1) {
    int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
    if (nfds < 0) {
      if (errno == EINTR)
        continue;
      perror("epoll_wait failed");
      break;
    }

    for (int i = 0; i < nfds; i++) {
      int fd = events[i].data.fd;
      uint32_t ev_flags = events[i].events;

      // Handle client disconnect or error
      if (ev_flags & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
        printf("[Server] Socket fd %d closed or errored\n", fd);
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        continue;
      }

      if (fd == listen_fd) {
        // New connection incoming
        handle_accept(epoll_fd, listen_fd);
      } else if (ev_flags & EPOLLIN) {
        // Existing client sent data
        handle_client_data(epoll_fd, fd);
      }
    }
  }

  close(listen_fd);
  close(epoll_fd);
  return 0;
}