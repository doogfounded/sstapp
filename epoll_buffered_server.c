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
#define BUFFER_SIZE 4096

typedef struct {
  int fd;
  char write_buf[BUFFER_SIZE];
  size_t write_len;
  size_t write_pos;
} client_state_t;

int set_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1)
    return -1;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// Clean up client memory and remove from epoll
void free_client(int epoll_fd, client_state_t *client) {
  printf("[Server] Closing client connection on fd %d\n", client->fd);
  epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client->fd, NULL);
  close(client->fd);
  free(client);
}

// Create and bind non-blocking IPv4 TCP server socket
int create_server_socket(int port) {
  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd == -1)
    return -1;

  int opt = 1;
  setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in address;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(port);

  if (bind(listen_fd, (struct sockaddr *)&address, sizeof(address)) < 0 ||
      listen(listen_fd, SOMAXCONN) < 0 || set_nonblocking(listen_fd) < 0) {
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

    int conn_fd =
        accept(listen_fd, (struct sockaddr *)&client_addr, &client_len);
    if (conn_fd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        break;
      perror("accept error");
      break;
    }

    if (set_nonblocking(conn_fd) < 0) {
      close(conn_fd);
      continue;
    }

    client_state_t *client = calloc(1, sizeof(client_state_t));
    client->fd = conn_fd;

    // Pass pointer to client_state_t in epoll_data
    struct epoll_event ev;
    ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
    ev.data.ptr = client;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, conn_fd, &ev) < 0) {
      perror("epoll_ctl: conn_fd");
      free(client);
      close(conn_fd);
    } else {
      printf("[Server] New client connected on fd %d\n", conn_fd);
    }
  }
}

// Attempt to send accumulated write buffer bytes
void flush_write_buffer(int epoll_fd, client_state_t *client) {
  while (client->write_pos < client->write_len) {
    ssize_t n = write(client->fd, client->write_buf + client->write_pos,
                      client->write_len - client->write_pos);

    if (n > 0) {
      client->write_pos += n;
    } else if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // Kernel send buffer full - subscribe to EPOLLOUT to try again later
        struct epoll_event ev;
        ev.events = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;
        ev.data.ptr = client;
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client->fd, &ev);
        return;
      }
      // Real write error
      perror("write error");
      free_client(epoll_fd, client);
      return;
    }
  }

  // Buffer fully drained: reset offsets and stop watching EPOLLOUT
  client->write_len = 0;
  client->write_pos = 0;

  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
  ev.data.ptr = client;
  epoll_ctl(epoll_fd, EPOLL_CTL_MOD, client->fd, &ev);
}

// Read incoming data into the client's write buffer
void handle_client_read(int epoll_fd, client_state_t *client) {
  char read_temp[BUFFER_SIZE];

  while (1) {
    ssize_t bytes_read = read(client->fd, read_temp, sizeof(read_temp));

    if (bytes_read > 0) {
      // Check if buffer has capacity
      size_t available_space = sizeof(client->write_buf) - client->write_len;
      size_t to_copy =
          (bytes_read < available_space) ? bytes_read : available_space;

      memcpy(client->write_buf + client->write_len, read_temp, to_copy);
      client->write_len += to_copy;

      // Attempt to send immediately
      flush_write_buffer(epoll_fd, client);
    } else if (bytes_read == 0) {
      // Client closed connection gracefully
      free_client(epoll_fd, client);
      break;
    } else {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        break; // Fully read for now
      perror("read error");
      free_client(epoll_fd, client);
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
    close(listen_fd);
    exit(EXIT_FAILURE);
  }

  // Allocate sentinel client_state_t for listening socket
  client_state_t listen_client = {.fd = listen_fd};

  struct epoll_event ev;
  ev.events = EPOLLIN | EPOLLET;
  ev.data.ptr = &listen_client;

  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) {
    perror("epoll_ctl: listen_fd");
    close(listen_fd);
    close(epoll_fd);
    exit(EXIT_FAILURE);
  }

  struct epoll_event events[MAX_EVENTS];
  printf("[Server] Listening on port %d with EPOLLOUT write buffers...\n",
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
      client_state_t *client = (client_state_t *)events[i].data.ptr;
      uint32_t ev_flags = events[i].events;

      if (client->fd == listen_fd) {
        handle_accept(epoll_fd, listen_fd);
        continue;
      }

      // Handle errors or disconnects
      if (ev_flags & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
        free_client(epoll_fd, client);
        continue;
      }

      // Flush pending writes first when socket becomes writable
      if (ev_flags & EPOLLOUT) {
        flush_write_buffer(epoll_fd, client);
      }

      // Handle incoming data
      if (ev_flags & EPOLLIN) {
        handle_client_read(epoll_fd, client);
      }
    }
  }

  close(listen_fd);
  close(epoll_fd);
  return 0;
}