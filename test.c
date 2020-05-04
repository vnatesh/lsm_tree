#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <math.h>
#include <stdbool.h>
#include <getopt.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h> 
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "threadpool.h"
#include "MurmurHash3.h"

// Tunable constants
#define PAGE_SZ 4096
#define MAXTHREADS 64

static int filesize = 100;

struct entry {
    int key;
    int val;
    bool del;
};

int comparator(const void* e1, const void* e2);



int comparator(const void* e1, const void* e2) { 
    return (((struct entry*) e1)->key > ((struct entry*) e2)->key ? 1 : -1);
} 



int main(int argc, char* argv[]) {

	srand(time(NULL));

	struct entry* data = (struct entry*) malloc(filesize * sizeof(struct entry));

	for(int i = 0; i < 10; i++) {
	    char filename[256];
	    sprintf(filename, "data/file_%d.bin", i);

	    FILE* fp = fopen(filename , "wb");
	    if(!fp) {
	        printf("Error: Could not open file\n");
	        exit(1);
	    }

	    for(int j = 0; j < filesize; j++) {
	    	data[j].key = rand();
	    	data[j].val = rand();
	    	data[j].del = true;
	    }
	
	    qsort((void*) (data), (size_t) filesize, sizeof(struct entry), comparator);

	    fwrite(data, sizeof(struct entry), filesize, fp);
	    fclose(fp);
	}

	int file_num = 0;

    char f1[256];
    char f2[256];
    sprintf(f1, "data/file_%d.bin", file_num);
    sprintf(f2, "data/file_%d.bin", file_num+1);

    int fd1 = open(f1, O_RDONLY, S_IRUSR | S_IWUSR);
    int fd2 = open(f2, O_RDONLY, S_IRUSR | S_IWUSR);


    struct entry* m1 = mmap(NULL, filesize * sizeof(struct entry), PROT_READ, MAP_PRIVATE, fd1, 0);
    struct entry* m2 = mmap(NULL, filesize * sizeof(struct entry), PROT_READ, MAP_PRIVATE, fd2, 0);



    char outfile[256];
    sprintf(outfile, "data/out_%d.bin", file_num);
    int out = open(outfile, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
 	// lseek(out, (2 * filesize * sizeof(struct entry)) - 1 , SEEK_SET);
	// write(out, "", 1);
    ftruncate(out,(2 * filesize * sizeof(struct entry)) );
    struct entry* m_out = mmap(NULL, (2 * filesize * sizeof(struct entry)), PROT_READ | PROT_WRITE, MAP_SHARED, out, 0);

    // merge algorithm
    int i = 0;
    int j = 0;
    int k = 0;

	while (i < filesize && j < filesize) {
		if(m1[i].key <= m2[j].key) { 
			m_out[k] = m1[i];
            i++; 
        } else { 
            m_out[k] = m2[j];
            j++; 
        } 
        k++; 
    } 
  
	while (i < filesize) {
		m_out[k] = m1[i];
		i++; 
		k++; 
    } 
  
	while (j < filesize) {
		m_out[k] = m2[j];
		j++; 
		k++; 
    } 
  
    printf("\n\n\n");

    for(int x = 0; x < filesize; x++) {
        printf("%d : %d : %d\n", m1[x].key, m1[x].val, m1[x].del);
    }

    printf("\n\n\n");
    for(int x = 0; x < filesize; x++) {
        printf("%d : %d : %d\n", m2[x].key, m2[x].val, m2[x].del);
    }

    printf("\n\n\n");

    for(int x = 0; x < 2*filesize; x++) {
        printf("%d : %d : %d\n", m_out[x].key, m_out[x].val, m_out[x].del);
    }


    munmap(m1, filesize * sizeof(struct entry));
    close(fd1);
    munmap(m2, filesize * sizeof(struct entry));
    close(fd2);
    munmap(m_out, 2 * filesize * sizeof(struct entry));
    close(out);
    free(data);
    return 0;
}





