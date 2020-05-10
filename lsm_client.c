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
// static const char *socket_path = "ipc_socket";
#define socket_path "ipc_socket"

int recv_timeout(int socket_fd , double timeout);


int main(int argc, char *argv[]) {

    struct sockaddr_un addr;
    // char buf[20000];
    char buf[31];
    char line[31];
    int fd;
    int rc=0;

    FILE *fp = fopen(argv[1], "r");

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

    int rc2;
    int bytes_read;


    // while( (rc=read(STDIN_FILENO, buf, sizeof(buf))) > 0) {
    while(fgets(line, sizeof(line), fp) != NULL) {

        // fgets(line, sizeof(line), fp);
        int ret = send(fd, line, strlen(line), 0); 
        if (ret == -1) {

            fprintf(stderr, "Failure Sending Message\n");
            close(fd);
            exit(1);

        } else {

            bytes_read = recv_timeout(fd, 0.0001);
            // bytes_read = recv_timeout(socket_fd, htmlFile);
            if ( bytes_read <= 0 ) {
                printf("Either Connection Closed or Error\n");
            }
        }

        // if (write(fd, buf, rc) != rc) {
        //     if (rc > 0) {
        //         fprintf(stderr,"partial write");
        //     } else {
        //         perror("write error");
        //         exit(-1);
        //     }
        // }
        // while ((rc2 = read(fd, buf, sizeof(buf))) > 0) {
        //     printf("%s", buf);
        //     // sleep(5);
        // }

    }

    return 0;
}



int recv_timeout(int socket_fd , double timeout) {
    int size_recv;
    int total_size = 0;
    struct timeval begin;
    struct timeval now;
    char response[31];
    double timediff;
    
    // Make socket non blocking so we can serve multiple 
    // asynchronous connections without I/O blocking
    fcntl(socket_fd, F_SETFL, O_NONBLOCK);
     
    //start time
    gettimeofday(&begin , NULL);
     
    while(1) {

        gettimeofday(&now , NULL);
        //time elapsed in seconds
        timediff = (now.tv_sec - begin.tv_sec) + 1e-6 * (now.tv_usec - begin.tv_usec);
         
        // If some data has been received but the we've exceeded the timeout, then
        // stop receiving new data
        if( total_size > 0 && timediff > timeout ) {
            break;
        // If no data was received at all, then wait 2*timeout longer
        } else if(timediff > timeout*2) {
            break;
        }
         
        memset(response ,0 , 31);  // clear the response buffer to accept new data from server
        if((size_recv =  recv(socket_fd , response , 31 , 0) ) < 0) {
            // If nothing received wait another 0.1 sec
            usleep(1);
        } else {
            // write(htmlFile, response, size_recv); 
            printf("val %s %d\n",response , size_recv);
            total_size += size_recv;
            gettimeofday(&begin , NULL);
        }
    }
    
    return total_size;

}
