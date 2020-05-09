#include <sys/time.h>
#include <fcntl.h>
#include <time.h> 



#include <sys/socket.h>
#include <sys/un.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

//char *socket_path = "./socket";
static const char *socket_path = "ipc_socket";




int main(int argc, char *argv[]) {

    struct sockaddr_un addr;
    // char buf[20000];
    char buf[31];
    int fd,rc;

    if (argc > 1) {
        socket_path=argv[1];
    }

    if ( (fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket error");
        exit(-1);
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;

    if (*socket_path == '\0') {
        *addr.sun_path = '\0';
        strncpy(addr.sun_path+1, socket_path+1, sizeof(addr.sun_path)-2);
    } else {
        strncpy(addr.sun_path, socket_path, sizeof(addr.sun_path)-1);
    }

    if (connect(fd, (struct sockaddr*) &addr, sizeof(addr)) == -1) {
        perror("connect error");
        exit(-1);
    }

    FILE *fp = fopen(argv[1], "r");

    // while( (rc=read(STDIN_FILENO, buf, sizeof(buf))) > 0) {
    while(fgets(line, sizeof(line), fp) != NULL) {

        if (write(fd, buf, rc) != rc) {
            if (rc > 0) {
                fprintf(stderr,"partial write");
            } else {
                perror("write error");
                exit(-1);
            }
        }
    }

    return 0;
}



