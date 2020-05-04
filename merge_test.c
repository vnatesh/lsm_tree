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


#include <stdatomic.h>
// atomic_int done = 0;


// Tunable constants
#define PAGE_SZ 4096
// #define MAXTHREADS 64

struct entry {
    int key;
    int val;
    bool del;
};

struct file_pair {
	char f1[32];
	char f2[32];
	char out_f[32];
};


// gcc -g -O0 -pthread merge_test.c threadpool.c -o merge_test


static pthread_rwlock_t rwlock;
static int SZ_RATIO = 7;
static unsigned long long int M_BUFFER;
static int filesize = 10000000;

static int done = 0;

static threadpool_t* pool;


static int group_wait = 0;
static int* k_wait = NULL;
static pthread_mutex_t group_lock;
static pthread_mutex_t* k_locks;

void binary_merge(void* inp);
void run_test(threadpool_t* pool);
int comparator(const void* e1, const void* e2);
void k_way_merge(int start, int end);



int comparator(const void* e1, const void* e2) { 
    return (((struct entry*) e1)->key > ((struct entry*) e2)->key ? 1 : -1);
} 


int main(int argc, char* argv[]) {

	int group_len = (int) ceil(log2(SZ_RATIO));
    group_wait = 0;
    int n = SZ_RATIO;
    int* groups = (int*) malloc(sizeof(int) * group_len * 2);
    int curr = 0;

    // convert n to binary, find position where val is 1 and create group of size 2^position
    for(int i = 0; n > 0; i += 2) {

    	if(n % 2) {
    		printf("curr %d\n",curr );
    		groups[i] = curr;
	    	curr += ((int) pow(2, i/2));
	    	groups[i+1] = curr;
	    	group_wait++;
    	} else {
    		groups[i] = -1;
    		groups[i + 1] = -1;
    	}

    	n /= 2;
    }

    k_wait = (int*) malloc(sizeof(int) * group_wait);
    k_locks = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t) * group_wait); 
pthread_mutex_init(&lock, NULL) 

    // we start the merge process by starting work (merge) on the larger groups (size determined by MSB of SZ_RATIO in binary)
    // first so that the smaller groups can be merging simulatenously and possibly finish before the large group. Small group
    // threads can be running while large group is running 
    for(int i = (group_len*2) - 1; i > 0; i -= 2) {
    	if(groups[i] != -1) {
	    	printf("HEYY %d  %d %d\n", group_wait,	groups[i-1], groups[i]);
	    	k_way_merge(groups[i-1], groups[i]);
    	}
    }

    exit(1);
    while(group_wait != 0) {}

    // if(groups[0] != -1) {
    // 	binary_merge(conver to char ..."group_%d.bin" groups[0])
    // }

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

    pool = threadpool_create(MAX_THREADS, MAX_QUEUE, 0);

    if (pthread_mutex_init(&lock, NULL) != 0) { 
        printf("\nError: mutex init failed\n"); 
        return 1; 
    } 

    // if (pthread_rwlock_init(&rwlock, NULL) != 0) { 
    //     printf("\nError: rwlock init failed\n"); 
    //     return 1; 
    // } 

	done = SZ_RATIO / 2;

	int* q = (int*) malloc(done * sizeof(int));


	struct timeval start, end;
    double diff_t;


    gettimeofday (&start, NULL);

	for(int i = 0; i < SZ_RATIO-1; i+=2) {
		q[i/2] = i;
		// printf("%d\n", q);
		threadpool_add(pool, binary_merge, (void*) &q[i/2] , 1);
		// binary_merge((void*) &q[i/2]);
	}

	while(done != 0) {}
	// printf("done : %d\n ", done );

    gettimeofday (&end, NULL);
    diff_t = (((end.tv_sec - start.tv_sec)*1000000L
        +end.tv_usec) - start.tv_usec) / (1000000.0);
    printf("merge time : %f\n", diff_t); 


	pthread_mutex_destroy(&lock); 
    // pthread_rwlock_destroy(&rwlock);
    threadpool_destroy(pool, 1);
    free(data);


	return 0;
}




// Merge k = (end-start) files together in parallel. k (a) is guaranteed to be a power of 2.
// Launch a binary merge on each of log2(k) threads in parallel
void k_way_merge(int start, int end) {

    int k = end - start;

	if(k < 2) {
		// do nothing, you need at least k=2 files to merge
		return;
	}

	int ind = 0;
	int p_ind = 0;
	// k_wait = k / 2;

	struct file_pair** q = (struct file_pair**) malloc(sizeof(struct file_pair*) * ((int) log2(k)));
	q[ind] = (struct file_pair*) malloc(sizeof(struct file_pair) * (k/2));


	printf("Group %d\n\n", ind);
	for(int i = start; i < end; i+=2) {

	    printf("data/file_%d.bin  ", i);
	    printf("data/file_%d.bin  ", i+1);
	    printf("data/out_%d_%d.bin\n", i+1, ind);
		printf("\n\n");
	    sprintf(q[ind][p_ind].f1, "data/file_%d.bin", i);
	    sprintf(q[ind][p_ind].f2, "data/file_%d.bin", i+1);
	    sprintf(q[ind][p_ind].out_f, "data/out_%d_%d.bin", i+1, ind);
		p_ind++;
	} 

	ind++;
	int j = 2;
	
	printf("\n\n\n\n\n");

	while(ind < ((int) log2(k))) {

		p_ind = 0;
		q[ind] = (struct file_pair*) malloc(sizeof(struct file_pair) * (k/2));

		printf("Group %d\n\n", ind);
		for(int i = j; i < k; i += 2*j) {
			printf("data/out_%d_%d.bin  ", i + start - 1, ind - 1);
			printf("data/out_%d_%d.bin  ", i + start + j - 1, ind - 1);
			printf("data/out_%d_%d.bin\n", i + start + j - 1, ind);
			printf("\n\n");
		    sprintf(q[ind][p_ind].f1, "data/out_%d_%d.bin", i + start - 1, ind - 1);
		    sprintf(q[ind][p_ind].f2, "data/out_%d_%d.bin", i + start + j - 1, ind - 1);
		    sprintf(q[ind][p_ind].out_f, "data/out_%d_%d.bin", i + start + j - 1, ind);
		    p_ind++;
		}

		printf("\n\n\n\n\n");


		ind++;
		j *= 2;
	}
 
    for(int i = 0; i < ((int) log2(k)); i++) {
    	k_wait[g_id] = (int) pow(2, (((int) log2(k)) - i - 1));
    	for(int j = 0; j < (k/2); j++) {
    		if(strlen(q[i][j].f1) != 0) {
				// threadpool_add(pool, binary_merge, (void*) &q[i][j] , 1);
				binary_merge((void*) &q[i][j]);
			}
		}

		while(k_wait[g_id] != 0) {}
    }

	pthread_mutex_lock(&group_lock); 
	group_wait--;
  	pthread_mutex_unlock(&group_lock); 

}


void binary_merge(void* inp) {

	// int file_num = *((int *) file_no);
	struct file_pair f_pair = *((struct file_pair*) inp);
	printf("yo %s %s %s\n", f_pair.f1, f_pair.f2, f_pair.out_f);
	return;

    int fd1 = open(f_pair.f1, O_RDONLY, S_IRUSR | S_IWUSR);
    int fd2 = open(f_pair.f2, O_RDONLY, S_IRUSR | S_IWUSR);

    struct entry* m1 = mmap(NULL, filesize * sizeof(struct entry), PROT_READ, MAP_PRIVATE, fd1, 0);
    struct entry* m2 = mmap(NULL, filesize * sizeof(struct entry), PROT_READ, MAP_PRIVATE, fd2, 0);

    int out = open(f_pair.out_f, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

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
  
    // printf("\n\n\n\n\n\n");

    // printf("MANN %d %d\n\n", file_num, file_num + 1);
    // for(int x = 0; x < 2*filesize; x++) {
    //     printf("%d : %d : %d\n", m_out[x].key, m_out[x].val, m_out[x].del);
    // }

    munmap(m1, filesize * sizeof(struct entry));
    close(fd1);
    munmap(m2, filesize * sizeof(struct entry));
    close(fd2);
    munmap(m_out, 2 * filesize * sizeof(struct entry));
    close(out);

	pthread_mutex_lock(&k_locks[g_id]); 
	k_wait[g_id]--;
	// atomic_fetch_sub_explicit(&k_wait[g_id], 1, memory_order_relaxed);
	// printf("done is %d\n", k_wait[g_id]);
  	pthread_mutex_unlock(&k_locks[g_id]); 
}
