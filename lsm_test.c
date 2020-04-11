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

#include <sys/time.h> 
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <pthread.h>
#include "threadpool.h"
#include "gheap.h"

#define PAGE_SZ 4096
#define P 8000
#define E 9
#define B (PAGE_SZ / E)
#define M_BUFFER (P * B * E)
#define MAXTHREADS 64
#define parent(i) (i / 2)
#define left(i) (2*i)
#define right(i) ((2*i) + 1)



// M_buffer = P * B * E
// where B is the number of entries that fit into a disk page, P is the amount of main memory in terms of disk pages allocated to the buffer, and E is the average size of data entries

// In our dataset, all entries are of equal size i.e.

// 1 entry = (key,val) = int + int = 8 bytes
//  => E = 8
// B = page_size / entry_size = 4096 / 8 = 512
// P = 8 (for now)

// M_buffer = 8 * 8 * 512 = 32 KB

// struct event {
//  struct process* proc;
//  enum states oldState;
//  enum states newState;
//  enum transitions transition;
//  int timestamp;
// };

// struct queue {
//  struct node* head;
//  struct node* tail;
//  int size;
// };

// struct node {
//  void* val;
//  struct node* next;
// };

// struct prioQueue {
//  struct queue** active;
//  struct queue** expired;
// };

// static const char delims[] = " \t\n";
// static int quantum = 10000;
// static int ofs = 0;
// static int* randvals;
// static int randCount;
// static int totalIO = 0;
// static int numProcs = 0;
// static int CURRENT_TIME;
// static bool CALL_SCHEDULER = false;
// static bool debug = false; // debug flag for printing each event
// static struct process* CURRENT_RUNNING_PROCESS = NULL;
// static enum schedulers sched;
// static void* runQueue = NULL;

enum command {PUT, GET, RANGE, DELETE};

struct query {
    enum command type;
    int inp1;
    int inp2;
};

struct entry {
    bool del;
    int key;
    int val;
};


struct buffer {
    struct entry* data;
    int heap_size;
    int length;
};


static const char delims[] = " ";
static pthread_rwlock_t rwlock;
static int SZ_RATIO;
static struct buffer* L0;

int load_queries(struct query* queries, char* filename);
void buffer_init();
void buffer_insert(struct query q);
void buffer_heapify(int i);
int comparator(const void* e1, const void* e2);
void buffer_qsort();
void buffer_heapsort1();

void buffer_load(struct query* queries);
void buffer_flush();

int get_parent(int u);
int get_child(int u);

void buffer_insert_new(const struct gheap_ctx *const ctx, struct query q);
void buffer_load_new(const struct gheap_ctx *const ctx, struct query* queries);













static int less(const void *const ctx, const void *const a, const void *const b) {

    (void)ctx;
    // return *(int *)a < *(int *)b;
    return (((struct entry*) a)->key < ((struct entry*) b)->key);
}


static void move(void* dst, const void *const src)
{
  // *(int *)dst = *(int *)src;
    *(struct entry*)dst = *(struct entry*) src;
}

static void heapsort1(const struct gheap_ctx *const ctx,
    const struct entry* a, const size_t n)
{
  // gheap_make_heap(ctx, (void* const) a, n);
  gheap_sort_heap(ctx, (void* const) a, n);
}






// write optimized buffer...insert O(1) (average case), delete O(lgn), but reads (search) is O(n)
// Buffer is a b-heap data structure meant to minimize page accesses while traversing through the heap.
void buffer_init() {

    L0 = (struct buffer*) malloc(sizeof(struct buffer));
    L0->data = (struct entry*) malloc(sizeof(struct entry) * P * (PAGE_SZ / sizeof(struct entry))); // allocate extra start entry for heap so indices are easier to deal with
    L0->heap_size = 0;
    L0->length = P * (PAGE_SZ / sizeof(struct entry));
    L0->data[0].key = -1; // dummy values for entry at index 0
    L0->data[0].val = -1;    
}


// insert into buffer heap
void buffer_insert(struct query q) {

    L0->heap_size++;

    if(q.type == PUT) {
        L0->data[L0->heap_size].key = q.inp1;
        L0->data[L0->heap_size].val = q.inp2;
        L0->data[L0->heap_size].del = false;
    } else {
        L0->data[L0->heap_size].key = q.inp1;
        L0->data[L0->heap_size].del = true;
    }

    int i = L0->heap_size;
    struct entry tmp;

    while(i > 1 && L0->data[get_parent(i)].key < L0->data[i].key) {
        tmp = L0->data[get_parent(i)];
        L0->data[get_parent(i)] = L0->data[i];
        L0->data[i] = tmp;
        i = get_parent(i);
    }
}


void buffer_heapify(int i) {

    int l = get_child(i);
    int r = l + 1;
    int max;
    struct entry tmp;

    if((l < L0->heap_size) && (L0->data[l].key > L0->data[i].key)) {
        max = l;
    } else {
        max = i;
    }

    if((r < L0->heap_size) && (L0->data[r].key > L0->data[max].key)) {
        max = r;
    }

    if(max != i) {
        tmp = L0->data[i];
        L0->data[i] = L0->data[max];
        L0->data[max] = tmp;
        buffer_heapify(max);
    }
}


void buffer_heapsort1() {

    struct entry tmp;
    int m = L0->heap_size;

    for(int i = m + 1; i > 1; i--) {
        tmp = L0->data[L0->heap_size];
        L0->data[L0->heap_size] = L0->data[1];
        L0->data[1] = tmp;
        L0->heap_size--;
        buffer_heapify(1);
    }

    L0->heap_size = m;
}


// void buffer_heapsort1(int m) {

//     struct entry tmp;

//     for(int i = m; i > 1; i--) {
//         tmp = L0->data[m];
//         L0->data[m] = L0->data[1];
//         L0->data[1] = tmp;
//         L0->heap_size--;
//         buffer_heapify(1);
//     }
//     L0->heap_size = m;
// }


void buffer_qsort() {
    qsort((void*) (L0->data + 1), (size_t) L0->heap_size, sizeof(struct entry), comparator); 
}


int comparator(const void* e1, const void* e2) { 

    return (((struct entry*) e1)->key > ((struct entry*) e2)->key ? 1 : -1);
} 

// If multiple runs that are being sort-merged contain entries with the same key, 
// only the entry from the most recently-created (youngest) run is kept because it 
// is the most up-to-date. Thus, the resulting run may be smaller than the cumulative sizes of the original runs. When
// a merge operation is finished, the resulting run moves to Level i + 1
// if Level i is at capacity.

void buffer_load(struct query* queries) {
    for(int i = 0; i < 2000210; i++) {
        if(queries[i].type == PUT || queries[i].type == DELETE) {
            buffer_insert(queries[i]);
        }
    }
}


void buffer_insert_new(const struct gheap_ctx *const ctx, struct query q) {

    L0->heap_size++;

    if(q.type == PUT) {
        L0->data[L0->heap_size].key = q.inp1;
        L0->data[L0->heap_size].val = q.inp2;
        L0->data[L0->heap_size].del = false;
    } else {
        L0->data[L0->heap_size].key = q.inp1;
        L0->data[L0->heap_size].del = true;
    }

    gheap_push_heap(ctx, (L0->data + 1),  L0->heap_size); // load in L0+1 index. Index 0 is a dummy
}

void buffer_load_new(const struct gheap_ctx *const ctx, struct query* queries) {

    for(int i = 0; i < 2000210; i++) {
        if(queries[i].type == PUT || queries[i].type == DELETE) {
            buffer_insert_new(ctx, queries[i]);
        }
    }
}


void buffer_flush() {

    free(L0->data);
    free(L0);

    // FILE* fp = fopen(filename,"r");
    // if(!fp) {
    //     printf("Error: Could not open file\n");
    //     exit(1);
    // }
}

int main(int argc, char* argv[]) {

    struct timeval start, end;
    double diff_t;

    // MAXTHREADS = atoi(argv[1]);
    struct query* queries = (struct query*) malloc(sizeof(struct query) * 2000210);
    buffer_init();
    int req_cnt = load_queries(queries, argv[1]);

    gettimeofday (&start, NULL);
    buffer_load(queries);
    printf("heap size : %d\n", L0->heap_size);
    gettimeofday (&end, NULL);
    diff_t = (((end.tv_sec - start.tv_sec)*1000000L
        +end.tv_usec) - start.tv_usec) / (1000000.0);
    printf("old buffer load time: %f\n", diff_t); 
    // printf("\n\n\n\n");

    static const struct gheap_ctx binary_heap_ctx = {
        .fanout = 2,
        .page_chunks = (PAGE_SZ / (sizeof(struct entry) * 2)),
        .item_size = sizeof(struct entry),
        .less_comparer = &less,
        .less_comparer_ctx = NULL,
        .item_mover = &move,
    };

    buffer_flush();
    buffer_init();

    gettimeofday (&start, NULL);
    buffer_load_new(&binary_heap_ctx, queries);
    printf("heap size : %d\n", L0->heap_size);
    gettimeofday (&end, NULL);
    diff_t = (((end.tv_sec - start.tv_sec)*1000000L
        +end.tv_usec) - start.tv_usec) / (1000000.0);
    printf("new buffer load time: %f\n", diff_t); 
    // for(int i = 1; i <= L0->heap_size; i++) {
    //     printf("%d : %d : %d\n",L0->data[i].del, L0->data[i].key, L0->data[i].val);
    // }
    // exit(1);


    /* heapsort1 using binary heap */
    gettimeofday (&start, NULL);
    heapsort1(&binary_heap_ctx, (const struct entry *) (L0->data + 1), L0->heap_size);
    gettimeofday (&end, NULL);
    
    diff_t = (((end.tv_sec - start.tv_sec)*1000000L
        +end.tv_usec) - start.tv_usec) / (1000000.0);
    printf("new heapsort1 time: %f\n", diff_t); 
    // printf("\n\n\n\n");


    buffer_flush();
    buffer_init();
    buffer_load_new(&binary_heap_ctx, queries);

    gettimeofday (&start, NULL);
    buffer_qsort();
    gettimeofday (&end, NULL);
    diff_t = (((end.tv_sec - start.tv_sec)*1000000L
        +end.tv_usec) - start.tv_usec) / (1000000.0);
    printf("buffer qsort time: %f\n", diff_t); 
    printf("heap size : %d\n", L0->heap_size);

    // for(int i = 1; i <= L0->heap_size; i++) {
    //     printf("%d : %d : %d\n",L0->data[i].del, L0->data[i].key, L0->data[i].val);
    // }

    // when the buffer is full:
    //     -sort the array (check using either qsort or heapsort1)
    //     -flush to disk by doing
    //     fwrite(buf->data + 1, M_BUFFER, ); i.e. start at the 1st index (not 0th) 
                                    // of the struct and get all memory after that

        // printf("%d : %d : %d\n",queries[i].type, queries[i].inp1, queries[i].inp2);
    

    // pthread_rwlock_t rwlock;
    // pthread_rwlock_init(&rwlock,NULL);
    // // look for new clients always
    // for each client {
    //  // add their queries to the threadpool 
    //     threadpool_add(pool, http_proxy, (void*) requests[i].url, 1);
    // }


    // pthread_rwlock_destroy(&rwlock);
    // threadpool_destroy(pool, 1);

    free(L0->data);
    free(L0);
    free(queries);
    return 1;
}


// load queries statically from a file
int load_queries(struct query* queries, char* filename) {

    char line[31];
    int req_cnt = 0;
    FILE *fp = fopen(filename, "r");
    char* a;

    while(fgets(line, sizeof(line), fp) != NULL) {

        if(line[0] != '\n') {

            struct query* q = (struct query*) malloc(sizeof(struct query));
            a = strtok(line, delims);

            switch(a[0]) {
                case 'p': 
                    q->type = PUT;
                    q->inp1 = atoi(strtok(NULL, delims));
                    q->inp2 = atoi(strtok(NULL, delims));
                    break;
                case 'r': 
                    q->type = RANGE;
                    q->inp1 = atoi(strtok(NULL, delims));
                    q->inp2 = atoi(strtok(NULL, delims));
                    break; 
                case 'd': 
                    q->type = DELETE;
                    q->inp1 = atoi(strtok(NULL, delims));
                    break;
                case 'g': 
                    q->type = GET;
                    q->inp1 = atoi(strtok(NULL, delims));
                    break; 
                // case 's':
                // case 'l':
                default: 
                    printf("Error : Invalid Command"); 
                    exit(1);
            }  
            
            queries[req_cnt] = *q;
            req_cnt++;
        }
    }

    fclose(fp);
    return req_cnt;
}


int get_parent(int u) {

    int page_chunks = PAGE_SZ / (sizeof(struct entry) * 2);
    --u;

    if (u < 4) {
        /* Parent is root. */
        return 1;
    }

    int page_size = 2 * page_chunks;
    int v = u % page_size;
    if (v >= 2) {
        // Parent is on the same page as the child. */
        return u - v + v / 2;
    }
    // Parent is on another page. */
    v = u / page_size - 1;
    int page_leaves = page_chunks + 1;
    u = v / page_leaves + 1;
    return u * page_size + v % page_leaves - page_leaves + 1;
}


int get_child(int u) {

    int page_chunks = PAGE_SZ / (sizeof(struct entry) * 2);
    if (u == 1) {
        /* Root's child is always 1. */
        return 2;
    }

    int page_size = 2 * page_chunks;
    --u;
    int v = u % page_size + 1;

    if (v < page_size / 2) {
        /* Fast path. Child is on the same page as the parent. */
        v *= 2 - 1;
        return u + v + 2;
    }

    /* Slow path. Child is on another page. */
    int page_leaves = (2 - 1) * page_chunks + 1;
    v += (u / page_size + 1) * page_leaves - page_size;
    return v * page_size + 1;
}




// ********************* NOTES ***********************

// cd cs265-sysproj/generator/; 
// ./generator --puts 2000000 --gets 100 --ranges 100 --deletes 10 --gets-misses-ratio 0.1 --gets-skewness 0.3 > b.txt; 
// mv b.txt ../..; 
// cd ../..;
// gcc -Wall -g -O0 -DNDEBUG -pthread lsm_test.c -o lsm_test
// ./lsm_test b.txt 


// Need to loptmiize bloom filter allocations per level (Monkey) and use a tiering policy for all levels
// except the last level where we use leveling (1 run Dotstoevsky)  

// Do locking per level or per run...If we do 1 lock per run, then the
// number of locks is size_ratio * lg(n) (n is the number of entries)
// follow cocurrent clocking scheme in https://www.cs.rochester.edu/u/scott/papers/1996_IPL_heaps.pdf

// updates and deletes are done on merging of the levels. They are not done in the buffer.

// We can use     pthread_rwlock_wrlock(&rwlock); 
// Before deleting unecessary read files, it will check if the rw_lock is being held by any reader threads (pthread_rwlock_rdlock(&rwlock)). 
// Wait until lock acquired. When we get the lock, delete files that had existed before merge


// How do we track disk pages in the database (for fence pointers) if malloc can create blocks of memory that straddle several pages?
// Can you exaplin the sentence ""


// ./generator --puts 200000 --gets 100 --ranges 100 --deletes 10 --gets-misses-ratio 0.1 --gets-skewness 0.2 | sort -R > test.txt

// sudo sysctl -a | grep cache

// exit_func():
//  when user wants to exit, flush bloom filters and fence points to disk as a file in some defined format


// startup():
//  if there is already data in the data directory, then load in the auxilliary bloom/fence data


