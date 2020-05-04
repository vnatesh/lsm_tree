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
	int group;
	int f1_size;
	int f2_size;
};


// gcc -g -O0 -pthread merge_test.c threadpool.c -o merge_test


static pthread_rwlock_t rwlock;
static int SZ_RATIO = 10;
static unsigned long long int M_BUFFER;
static int filesize = 1000;

static int done = 0;

static threadpool_t* pool;


static int group_wait = 0;
static int* k_wait = NULL;
static pthread_mutex_t group_lock;
static pthread_mutex_t* k_locks;

void binary_merge(void* inp);
void run_test(threadpool_t* pool);
int comparator(const void* e1, const void* e2);
struct file_pair* k_way_merge(int start, int end, int g_id);



int comparator(const void* e1, const void* e2) { 
    return (((struct entry*) e1)->key > ((struct entry*) e2)->key ? 1 : -1);
} 


int main(int argc, char* argv[]) {

	struct entry* data = (struct entry*) malloc(filesize * sizeof(struct entry));

	for(int i = 0; i < SZ_RATIO; i++) {
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

    // if (pthread_rwlock_init(&rwlock, NULL) != 0) { 
    //     printf("\nError: rwlock init failed\n"); 
    //     return 1; 
    // } 

    // group_len is the maximum number of bits i.e. groups we would need to track
    // g_cnt is the actual number of groups that we track i.e. where bit is 1
	int group_len = (int) (floor(log2(SZ_RATIO)) + 1);
    group_wait = 0;
    int n = SZ_RATIO;
    int* groups = (int*) malloc(sizeof(int) * group_len * 2);
    int curr = 0;
    int g_cnt = 0;

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

    g_cnt = group_wait;
    k_wait = (int*) malloc(sizeof(int) * g_cnt);
    k_locks = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t) * g_cnt);

    for(int i = 0; i < g_cnt; i++) {
	    if (pthread_mutex_init(&k_locks[i], NULL) != 0) { 
	        printf("\nError: mutex init failed\n"); 
	        return 1; 
	    }  
    } 


    // We start the merge process by starting work (merge) on the larger groups (size determined by MSB of SZ_RATIO in binary)
    // first so that the smaller groups can be merging simulatenously and possibly finish before the large group. Small group
    // threads can be running while large group is running
    int x = 0;
    struct file_pair* outs[g_cnt+1]; 

    for(int i = (group_len*2) - 1; i > 0; i -= 2) {
    	if(groups[i] != -1) {
	    	printf("HEYY %d  %d %d\n", group_wait,	groups[i-1], groups[i]);
	    	outs[x] = k_way_merge(groups[i-1], groups[i], x);
	    	printf("done with file %s and x is %d\n", outs[x]->out_f, x);
	    	x++;
    	}
    }

    // exit(1);


    while(group_wait != 0) {}

    int level = 3;
    int run = 4;
    struct file_pair output;
	char newname[32];

	// since SZ_RATIO is maxed at 10, at most 3 groups will be present (3 out of 4 bits turned on)
    if(g_cnt == 1) {
		sprintf(newname, "data/file_%d_%d.bin", level, run);
		rename(outs[0]->out_f, newname);
    
    } else if(g_cnt == 2) {

    	strcpy(output.f1, outs[1]->out_f);
    	strcpy(output.f2, outs[0]->out_f);
    	output.group = 0;
    	output.f1_size = outs[1]->f1_size + outs[1]->f2_size;
    	output.f2_size = outs[0]->f1_size + outs[0]->f2_size;
    	sprintf(output.out_f, "data/file_%d_%d", level, run);
    	binary_merge((void*) &output);
    	
    	printf("FINAL FILE %s\n", output.out_f);
    
    } else if(g_cnt == 3) {

    	strcpy(output.f1, outs[2]->out_f);
    	strcpy(output.f2, outs[1]->out_f);
    	output.group = 0;
    	output.f1_size = outs[2]->f1_size + outs[2]->f2_size;
    	output.f2_size = outs[1]->f1_size + outs[1]->f2_size;
    	sprintf(output.out_f, "data/intermediate");
    	binary_merge((void*) &output);

    	sprintf(output.f1, "data/intermediate");
    	strcpy(output.f2, outs[0]->out_f);
    	output.group = 0;
    	output.f1_size = outs[2]->f1_size + outs[2]->f2_size;
    	output.f2_size = outs[0]->f1_size + outs[0]->f2_size;
    	sprintf(output.out_f, "data/file_%d_%d", level, run);
    	binary_merge((void*) &output);

    	printf("FINAL FILE %s\n", output.out_f);
    }

    // rwlock write lock the levels table, and delete all old files and intermediate files, update
    // levels metadata
    // now unlock the rwlock

	// done = SZ_RATIO / 2;

	// int* q = (int*) malloc(done * sizeof(int));


	// struct timeval start, end;
 //    double diff_t;


 //    gettimeofday (&start, NULL);

	// for(int i = 0; i < SZ_RATIO-1; i+=2) {
	// 	q[i/2] = i;
	// 	// printf("%d\n", q);
	// 	threadpool_add(pool, binary_merge, (void*) &q[i/2] , 1);
	// 	// binary_merge((void*) &q[i/2]);
	// }

	// while(done != 0) {}
	// // printf("done : %d\n ", done );

 //    gettimeofday (&end, NULL);
 //    diff_t = (((end.tv_sec - start.tv_sec)*1000000L
 //        +end.tv_usec) - start.tv_usec) / (1000000.0);
 //    printf("merge time : %f\n", diff_t); 


    for(int i = 0; i < g_cnt; i++) {
    	pthread_mutex_destroy(&k_locks[i]); 
    }

	// pthread_mutex_destroy(&lock); 
    // pthread_rwlock_destroy(&rwlock);
    threadpool_destroy(pool, 1);
    free(data);

    free(k_locks);
    free(k_wait);
    free(groups);

	return 0;
}


// Merge k = (end-start) files together in parallel. k (a) is guaranteed to be a power of 2.
// Launch a binary merge on each of log2(k) threads in parallel
struct file_pair* k_way_merge(int start, int end, int g_id) {

    int k = end - start;

	if(k < 2) {
		// do nothing, you need at least k=2 files to merge
		struct file_pair* a = (struct file_pair*) malloc(sizeof(struct file_pair) * 1);
	    // sprintf(a->f1, "data/file_%d.bin", start);
	    // sprintf(a->f2, "data/file_%d.bin", start);
	    sprintf(a->out_f, "data/file_%d.bin", start);
	    a->group = g_id;

		return a;
	}

	int ind = 0;
	int p_ind = 0;

	struct file_pair** q = (struct file_pair**) malloc(sizeof(struct file_pair*) * ((int) log2(k)));
	q[ind] = (struct file_pair*) malloc(sizeof(struct file_pair) * (k/2));


	printf("sub-group %d\n\n", ind);
	for(int i = start; i < end; i+=2) {

	    printf("data/file_%d.bin  ", i);
	    printf("data/file_%d.bin  ", i+1);
	    printf("data/out_%d_%d.bin\n", i+1, ind);
		printf("\n\n");
	    sprintf(q[ind][p_ind].f1, "data/file_%d.bin", i);
	    sprintf(q[ind][p_ind].f2, "data/file_%d.bin", i+1);
	    sprintf(q[ind][p_ind].out_f, "data/out_%d_%d.bin", i+1, ind);
	    q[ind][p_ind].group = g_id;

	    q[ind][p_ind].f1_size = filesize;
	    q[ind][p_ind].f2_size = filesize;

		p_ind++;
	} 

	ind++;
	int j = 2;
	
	printf("\n\n\n\n\n");

	while(ind < ((int) log2(k))) {

		p_ind = 0;
		q[ind] = (struct file_pair*) malloc(sizeof(struct file_pair) * (k/2));

		printf("sub-group %d\n\n", ind);
		for(int i = j; i < k; i += 2*j) {
			printf("data/out_%d_%d.bin  ", i + start - 1, ind - 1);
			printf("data/out_%d_%d.bin  ", i + start + j - 1, ind - 1);
			printf("data/out_%d_%d.bin\n", i + start + j - 1, ind);
			printf("\n\n");
		    sprintf(q[ind][p_ind].f1, "data/out_%d_%d.bin", i + start - 1, ind - 1);
		    sprintf(q[ind][p_ind].f2, "data/out_%d_%d.bin", i + start + j - 1, ind - 1);
		    sprintf(q[ind][p_ind].out_f, "data/out_%d_%d.bin", i + start + j - 1, ind);
		    q[ind][p_ind].group = g_id;

    	    q[ind][p_ind].f1_size = filesize * j;
		    q[ind][p_ind].f2_size = filesize * j;

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
				threadpool_add(pool, binary_merge, (void*) &q[i][j] , 1);
				// binary_merge((void*) &q[i][j]);
			}
		}

		while(k_wait[g_id] != 0) {}
    }

	pthread_mutex_lock(&group_lock); 
	group_wait--;
  	pthread_mutex_unlock(&group_lock); 
    
    return &(q[((int) log2(k)) - 1][0]);
}


void binary_merge(void* inp) {

	struct file_pair f_pair = *((struct file_pair*) inp);
	printf("yo %s %s %s %d %d %d\n", f_pair.f1, f_pair.f2, f_pair.out_f, f_pair.f1_size, f_pair.f2_size, f_pair.group);
	// return;

    // int inp_size1 = filesize * sizeof(struct entry) * f_pair.f1_size;
    // int inp_size2 = filesize * sizeof(struct entry) * f_pair.f2_size;
    // int out_size = inp_size1 + inp_size2;

    int inp_size1 = f_pair.f1_size * sizeof(struct entry);
    int inp_size2 = f_pair.f2_size * sizeof(struct entry);
    int out_size = inp_size1 + inp_size2;


    int fd1 = open(f_pair.f1, O_RDONLY, S_IRUSR | S_IWUSR);
    int fd2 = open(f_pair.f2, O_RDONLY, S_IRUSR | S_IWUSR);

    struct entry* m1 = mmap(NULL, inp_size1, PROT_READ, MAP_PRIVATE, fd1, 0);
    struct entry* m2 = mmap(NULL, inp_size2, PROT_READ, MAP_PRIVATE, fd2, 0);

    int out = open(f_pair.out_f, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);

    ftruncate(out, out_size);

    // create output mmap of size equal to sum of input files
    struct entry* m_out = mmap(NULL, out_size, PROT_READ | PROT_WRITE, MAP_SHARED, out, 0);

    // merge algorithm. 
    // TODO : Do updates and deletes here
    int i = 0;
    int j = 0;
    int k = 0;

	while (i < f_pair.f1_size && j < f_pair.f2_size) {
		if(m1[i].key <= m2[j].key) { 
			m_out[k] = m1[i];
            i++; 
        } else { 
            m_out[k] = m2[j];
            j++; 
        } 
        k++; 
    } 
  
	while (i < f_pair.f1_size) {
		m_out[k] = m1[i];
		i++; 
		k++; 
    } 
  
	while (j < f_pair.f2_size) {
		m_out[k] = m2[j];
		j++; 
		k++; 
    } 
  
    // printf("\n\n\n\n\n\n");

    // printf("MANN %d %d\n\n", file_num, file_num + 1);
    // for(int x = 0; x < 2*filesize; x++) {
    //     printf("%d : %d : %d\n", m_out[x].key, m_out[x].val, m_out[x].del);
    // }

    munmap(m1, inp_size1);
    close(fd1);
    munmap(m2, inp_size2);
    close(fd2);
    munmap(m_out, out_size);
    close(out);

	pthread_mutex_lock(&k_locks[f_pair.group]); 
	k_wait[f_pair.group]--;
	// atomic_fetch_sub_explicit(&k_wait[g_id], 1, memory_order_relaxed);
	// printf("done is %d\n", k_wait[g_id]);
  	pthread_mutex_unlock(&k_locks[f_pair.group]); 
}
