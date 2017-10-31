#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h> // for close
#include <assert.h> // for assert
#include <stdio.h> // for printf
#include <string.h> // for strncmp

static const uint16_t NBD_FLAG_HAS_FLAGS = 0;

void read_all(int sock, void* buf, int length)
{
  assert(recv(sock, buf, length, MSG_WAITALL) == length);
}

void expect(int sock, char* buf, char* str)
{
  int length = strlen(str);
  read_all(sock, buf, length);
  assert(strncmp(str, buf, length) == 0);
  printf("Received %s\n", buf);
}

void fixed_newstyle_handshake(int sock, char* buf)
{
  expect(sock, buf, "NBDMAGIC");
  expect(sock, buf, "IHAVEOPT");
  uint16_t handshake_flags;
  read_all(sock, &handshake_flags, sizeof(uint16_t));
  handshake_flags = ntohs(handshake_flags);
  printf("Received handshake flags: %d\n", handshake_flags);
  assert((handshake_flags & NBD_FLAG_HAS_FLAGS) == NBD_FLAG_HAS_FLAGS);
}

int main(int argc, char* argv[])
{
  int sock;
  struct sockaddr_in server;
  char* address;
  int port = 10809;
  int block_size = 4 * 1024 * 1024;

  assert(argc == 2);
  address = argv[1];

  sock = socket(AF_INET, SOCK_STREAM, 0);
  server.sin_addr.s_addr = inet_addr(address);
  server.sin_family = AF_INET;
  server.sin_port = htons(port);

  assert(connect(sock, (struct sockaddr*)&server, sizeof(server)) == 0);
  printf("Connected to address %s and port %d\n", address, port);
  char buf[block_size];

  fixed_newstyle_handshake(sock, buf);

  // SEGFAULT:
  puts((char*)432);
  close(sock);
  return 0;
}
