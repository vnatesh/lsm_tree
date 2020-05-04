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
// buffer heap functions
#define parent(i) (i / 2)
#define left(i) (2*i)
#define right(i) ((2*i) + 1)
// Bloom filter functions
#define SetBit(A,k)     ( A[(k/8)] |= (1 << (k%8)) )
#define ClearBit(A,k)   ( A[(k/8)] &= ~(1 << (k%8)) )            
#define TestBit(A,k)    ( A[(k/8)] & (1 << (k%8)) )

enum command {PUT, GET, RANGE, DELETE};
enum policy {TIERED, LEVELED, LAZY_LEVELED};

struct query {
    int inp1;
    int inp2;
    enum command type;
};

struct entry {
    int key;
    int val;
    bool del;
};

struct buffer {
    struct entry* data;
    int heap_size;
    int length;
};

struct fence{
    int min;
    int max;
}

struct run {
    struct fence* fences;
    char* file;
    char* bloom;
    int bits_per_entry;
    int num_hash;
}

struct level {
    struct run* runs;
    int run_cnt;
    int bits_per_entry; // # of bits per entry in bloom filter
    int entries_per_run; // # of entries per run. This is the max since some runs will have less due to deletes/updates
    enum policy pol;
}

static const char delims[] = " ";
static pthread_rwlock_t rwlock;
static int SZ_RATIO;
static unsigned long long int M_BUFFER;
static char* static_workload = NULL;
static int NUM_CLIENTS;

static struct buffer* L0;
static struct level* levels;

static unsigned long long int MAX_MEM;
static int NUM_LEVELS;
static int L1_BPE;

// L0 buffer functions
void buffer_init();
void buffer_heapify(int i);
int comparator(const void* e1, const void* e2);
void buffer_qsort();
void buffer_load(char* filename);
void buffer_insert(struct query q);
void buffer_free();
void buffer_flush();

// bloom filter functions
void build_filter(int* arr, char* bloom, int num_hash);
void bloom_set(char* bloom, int key, int num_hash);
bool bloom_test(char* bloom, int key, int num_hash);


// This function initializes metadata for each level in the LSM tree according to 
// user defined policies (leveling, tiering, lazy leveling). It also implements the 
// Monkey optimizations for bloom filters
// TODO : need to support leveling, tiering, or lazy-leveling policies for merge in each level
void levels_init() {
    // L = ln((N*E / M_BUF) * ((T-1) / T)) / ln(T)
    NUM_LEVELS = (int) ceil(log((((double) MAX_MEM) / ((double) M_BUFFER)) * 
                (((double) (SZ_RATIO-1)) / ((double) SZ_RATIO))) / log(SZ_RATIO));

    levels = (struct levels*) malloc(sizeof(level) * NUM_LEVELS);

    // user supplies bits per entry for level 1 (L1_BPE)
    double p1 = exp(-L1_BPE * pow(log(2),2));
    // get the # of levels for which we assign bloom filters. All levels >= this one will not be given any filters
    int filtered_levels =  (int) floor((((double) L1_BPE) * pow(log(2),2)) / (log(SZ_RATIO)));
    int bits_per_entry;
    int num_hash;
    int entries_per_run;
    // int extra;

    // below loop is just for a tiering policy, allocates SZ_RATIO runs
    // TODO : make this a separate function and create another function for the leveling policy in a run. 
    // User should indicate what type of policy to use in each level (maybe via some kind of struct). 
    for(int i = 0; i < NUM_LEVELS; i++) {

        levels[i]->runs = (struct run*) malloc(sizeof(run) * SZ_RATIO);
        levels[i]->run_cnt = 0;
        levels[i]->entries_per_run = (L0->length * pow(SZ_RATIO, i));

        if(i < filtered_levels) {
            for(int j = 0; j < SZ_RATIO; j++) {
                // set size of bloom filter based on optimization in Monkey
                bits_per_entry = (int) ceil(-log(pow(SZ_RATIO, i) * p1) / pow(log(2),2));
                num_hash = (int) ceil(((double) bits_per_entry) * log(2));
                
                // if there are remainder bits, then add an extra byte 
                // extra = (((M_BUFFER * pow(SZ_RATIO, i)) * bits_per_entry) % 8 == 0 ? 0 : 1);

                levels[i]->runs[j]->file = NULL;
                levels[i]->runs[j]->num_hash = num_hash;
                levels[i]->runs[j]->bits_per_entry = bits_per_entry;
                // levels[i]->runs[j]->bloom = (char*) malloc((((M_BUFFER * pow(SZ_RATIO, i)) * bits_per_entry) / 8) + extra);
                levels[i]->runs[j]->bloom = (char*) malloc((levels[i]->entries_per_run * bits_per_entry) / 8);
                levels[i]->runs[j]->fences = (struct fence*) malloc(sizeof(struct fence) * 
                                            ((M_BUFFER * pow(SZ_RATIO, i)) / PAGE_SZ) );
            }
        } else {
            for(int j = 0; j < SZ_RATIO; j++) {

                levels[i]->runs[j]->file = NULL;
                levels[i]->runs[j]->bits_per_entry = 0;
                levels[i]->runs[j]->num_hash = 0;
                levels[i]->runs[j]->bloom = NULL;
                levels[i]->runs[j]->fences = (struct fence*) malloc(sizeof(struct fence) * 
                                            ((M_BUFFER * pow(SZ_RATIO, i)) / PAGE_SZ) );
            }
        }
    }
}


// From "Less Hashing, Same Performance: Building a Better Bloom Filter Adam Kirsch, Michael Mitzenmacher"
// http://citeseerx.ist.psu.edu/viewdoc/download;jsessionid=95B8140B9772E2DDB24199DA24DE5DB1?doi=10.1.1.152.579&rep=rep1&type=pdf
// gi(x) = h1(x) + ih2(x)
void bloom_set(int key, int i, int j) {

    uint64_t out[2];
    int h = levels[i]->runs[j]->num_hash;
    int n = levels[i]->entries_per_run;
    int b = levels[i]->runs[j]->bits_per_entry;

    MurmurHash3_x64_128(&key, sizeof(int), 0, &out);

    for(int i = 0; i < h; i++) {
        SetBit(levels[i]->runs[j]->bloom, ((out[0] + i * out[1]) % (n * b)));
    } 
}


bool bloom_test(int key, int i, int j) {

    uint64_t out[2];
    int h = levels[i]->runs[j]->num_hash;
    int n = levels[i]->entries_per_run;
    int b = levels[i]->runs[j]->bits_per_entry;

    MurmurHash3_x64_128(&key, sizeof(int), 0, &out);

    for(int i = 0; i < h; i++) {
        if(!TestBit(levels[i]->runs[j]->bloom, ((out[0] + i * out[1]) % (n * b)))) {
            return false;
        }
    }

    return true;
}


void build_filter(int* arr, char* bloom, int num_hash) {

    for(int i = 0; i < num_entries; i++) {
         bloom_set(bloom, arr[i], num_hash);
    }
}



    double x = ceil(bits_per_entry * log(2));
    x = x + 0.5 - (x<0); // x is now 55.499999...
    int num_hash = (int) x; // truncated to 55
    // printf("num_hashes%d\n", num_hash);
    int* arr = (int*) malloc(sizeof(int) * num_entries);
    for(int i = 0; i < num_entries; i++) {
        arr[i] = i;
    }

    char* bloom = (char*) malloc((num_entries * bits_per_entry) / 8); 
    build_filter(arr, bloom, num_hash);


    srand(time(NULL));
    int fp_cnt = 0;
    int a;
    for(int i = 0; i < num_entries; i++) {
        a = rand();
        if(bloom_test(bloom, a, num_hash)) {
            if(a >= num_entries) {
                printf("FP : %d\n", a);
                fp_cnt++;
            }
        }
    }

    printf("\n\n FPR : %.9lf\n", ((double) fp_cnt) / ((double) num_entries));

// write optimized buffer...insert O(1) (average case), delete O(lgn), but reads (search) is O(n)
void buffer_init() {

    L0 = (struct buffer*) malloc(sizeof(struct buffer));
    L0->data = (struct entry*) malloc(M_BUFFER + sizeof(struct entry)); // allocate extra start entry for heap so indices are easier to deal with
    L0->heap_size = 0;
    L0->length = M_BUFFER / sizeof(struct entry);
    L0->data[0].key = -1; // dummy values for entry at index 0
    L0->data[0].val = -1;    
}


void buffer_qsort() {
    qsort((void*) (L0->data + 1), (size_t) L0->heap_size, sizeof(struct entry), comparator); 
}


int comparator(const void* e1, const void* e2) { 
    return (((struct entry*) e1)->key > ((struct entry*) e2)->key ? 1 : -1);
} 



void buffer_flush() {

    char filename[256];
    sprintf(filename, "level1_run%d.bin", level[0]->run_cnt);

    FILE* fp = fopen(filename , "wb");
    if(!fp) {
        printf("Error: Could not open file\n");
        exit(1);
    }

    qsort((void*) (L0->data + 1), (size_t) L0->heap_size, sizeof(struct entry), comparator);

    fwrite(L0->data + 1, sizeof(struct entry), L0->length, fp);
    fclose(fp);

    // write lock the levels[0] table here
    levels[0]->run_cnt++;
    // write unlock the levels[0] table here
    L0->heap_size = 0;

    merge_and_flush(0) // launch merge/flush on separate thread so it happens in background
    

}


void merge_and_flush(int i) {
    
    // only flush to disk if num_files in level i == SZ_RATIO
    if(levels[0]->run_cnt < SZ_RATIO) {
        return; 
    } else {

    - get filenames of runs in level i
    - sort-merge the runs in level i via external sort
    - create new bloom filter and fence pointer array during merge
    - write new file to disk
    - lock levels table
    - remove file names of the runs in level i, memset(0) all the bloom filters in level i
    - add a new run in new filename in next level i+1, 

        r = level[i+1]->run_cnt
        level[i+1]->runs[r]->filename = new_filename;
        level[i+1]->run_cnt++;

        levels[i]->runs[run_cnt]->file = NULL;
        levels[i]->runs[j]->bloom = new_bloom;
        levels[i]->runs[j]->fences = (struct fence*) malloc(sizeof(struct fence) * 
                                    ((M_BUFFER * pow(SZ_RATIO, (i+1))) / PAGE_SZ) );

    - unlock levels table

        merge_and_flush(i+1) // cascade merge/flush
        
    }
}


int get(int key) {

    int val = 0;
    // linear O(n) search through buffer heap 
    for(int i = 0; i < L0->heap_size; i++) {
        if(key == L0->data[i].key) {
            return L0->data[i].val;
        }
    }
    // if not found in buffer, search LSM tree on disk
    for(int i = 0; i < NUM_LEVELS; i++) {
        for(int j = levels[i]->run_cnt; j > 0; j--) { // Go to most recent run first since it may contain updated keys
            if(bloom_test(key, i, j)) {
                if((val = search_fences(key, i, j)) != 0 ) {            
                    return val;
                }
            }
        }
    }

    return val;
}

// If multiple runs that are being sort-merged contain entries with the same key, 
// only the entry from the most recently-created (youngest) run is kept because it 
// is the most up-to-date. Thus, the resulting run may be smaller than the cumulative sizes of the original runs. When
// a merge operation is finished, the resulting run moves to Level i + 1
// if Level i is at capacity.
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

    while(i > 1 && L0->data[parent(i)].key < L0->data[i].key) {
        tmp = L0->data[parent(i)];
        L0->data[parent(i)] = L0->data[i];
        L0->data[i] = tmp;
        i = parent(i);
    }
}


// load queries statically from a file
void buffer_load(char* filename) {

    char line[31];
    FILE *fp = fopen(filename, "r");
    char* a;

    struct query q;

    while(fgets(line, sizeof(line), fp) != NULL) {

        if(L0->heap_size == L0->length) {
            buffer_flush();  
        }

        if(line[0] != '\n') {

            a = strtok(line, delims);

            switch(a[0]) {
                case 'p': 
                    q.type = PUT;
                    q.inp1 = atoi(strtok(NULL, delims));
                    q.inp2 = atoi(strtok(NULL, delims));
                    buffer_insert(q);
                    break;
                case 'd': 
                    q.type = DELETE;
                    q.inp1 = atoi(strtok(NULL, delims));
                    buffer_insert(q);
                    break;
                case 'g': 
                    q.type = GET;
                    q.inp1 = atoi(strtok(NULL, delims));
                    get(q.inp1)
                    break; 
                case 'r': 
                    q.type = RANGE;
                    q.inp1 = atoi(strtok(NULL, delims));
                    q.inp2 = atoi(strtok(NULL, delims));
                    break; 
                // case 's':
                // case 'l':
                default: 
                    printf("Error : Invalid Command"); 
                    exit(1);
            }   
        }
    }

    fclose(fp);
}


void buffer_free() {

    free(L0->data);
    free(L0);
}


// size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream)

int main(int argc, char* argv[]) {

    int opt;
    opterr = 0;
    struct timeval start, end;
    double diff_t;

    while((opt = getopt(argc, argv, "r:b:c:d:e:f::")) != -1) { // optional argument for static workload -f is denoted by ::
        switch(opt) {
            case 'r':
                SZ_RATIO = atoi(optarg);
                break;

            case 'b':
                M_BUFFER = strtoull(optarg) * (1 << 20); // convert MB to bytes
                break;

            case 'f':
                static_workload = optarg;
                break;

            case 'c':
                NUM_CLIENTS = atoi(optarg);
                break;

            case 'd':
                MAX_MEM = strtoull(optarg) * (1 << 20); // convert MB to bytes
                break;

            case 'e':
                L1_BPE = atoi(optarg); // bits per entry in L1
                break;

            case '?':
                if (optopt == 'r' || optopt == 'b' || optopt == 'f' || optopt == 'c'
                    || optopt == 'd' || optopt == 'e') {
                    fprintf(stderr, "Option -%c requires an argument.\n", optopt);
                    fprintf(stderr, "-r : size ratio\n");
                    fprintf(stderr, "-b : buffer size (MB)\n");
                    fprintf(stderr, "-f : static workload filename\n");
                    fprintf(stderr, "-c : # of clients\n");
                    fprintf(stderr, "-d : database size (MB)\n");
                    fprintf(stderr, "-e : # bits per entry in level 1 bloom filters\n");
                } else if(isprint(optopt)) {
                    fprintf(stderr, "Unknown option `-%c'.\n", optopt);
                } else {
                    fprintf(stderr, "Unknown option character `\\x%x'.\n", optopt);
                }
                return 1;

            default:
            abort();
        }
    }

    buffer_init();

    if(static_workload) {

        gettimeofday (&start, NULL);

        buffer_load(static_workload);

        printf("heap size : %d\n", L0->heap_size);
        gettimeofday (&end, NULL);
        diff_t = (((end.tv_sec - start.tv_sec)*1000000L
            +end.tv_usec) - start.tv_usec) / (1000000.0);
        printf("new buffer load time: %f\n", diff_t); 
        printf("\n\n\n\n");
        // for(int i = 1; i <= L0->heap_size; i++) {
        //     printf("%d : %d : %d\n",L0->data[i].del, L0->data[i].key, L0->data[i].val);
        // }

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
    }

    buffer_free();

    return 1;
}



    // memset(bloom_f, 0, N)
    // struct stat sb;

    // // int fd = fileno(fp);
    // int fs = open(file)
    // if(fstat(fd, &sb) == -1) {
    //     perror("Error: Could not open file\n");
    // }
    // printf("file size is %lld\n", sb.st_size);

    // char* file_in_memory = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    // for(int i = 0; i < sb.st_size; i++) {
    //     printf("%c", file_in_memory[i]);
    // }
    // printf("\n");

    // munmap(file_in_memory, sb.st_size);
    // fclose(fp);
    // exit(1);

    // get size of the ss_table index block = ind_block_size
    // char* file_in_memory = mmap(NULL, ind_block_size, PROT_READ, MAP_PRIVATE fd, 0);
    // If you find the desired index in on of the block ranges during a read (GET),
    //      read that chunk of the file starting at desired offset 

// void *mmap(void *addr, size_t lengthint " prot ", int " flags ,
//            int fd, off_t offset);int munmap(void *addr, size_t length);

// int munmap(void *addr, size_t length);

    // MAXTHREADS = atoi(argv[1]);

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

// ********************* NOTES ***********************


// cd cs265-sysproj/generator/; 
// ./generator --puts 20000 --gets 100 --ranges 100 --deletes 1--gets-misses-ratio 0.1 --gets-skewness 0.2 > b.txt; 
// mv b.txt ../..; 
// cd ../..;
// gcc -Wall -g -O0 -DNDEBUG -pthread lsm_test.c -o lsm_test
// ./lsm_test b.txt 

// gcc -Wall -g -O0 -DNDEBUG -pthread lsm.c -o lsm
// ./lsm -r2 -b32 -ftest.txt -c1



// heap size : 200010
// old buffer load time: 0.019460
// heap size : 200010
// new buffer load time: 0.016910
// new heapsort1 time: 0.152744
// buffer qsort time: 0.026340
// heap size : 200010

// heap size : 2000010
// old buffer load time: 0.188433
// heap size : 2000010
// new buffer load time: 0.159769
// new heapsort1 time: 2.302634
// buffer qsort time: 0.288512
// heap size : 2000010


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


// valgrind --leak-check=yes --track-origins=yes ./diophan

// The code deliverable of this project is an LSM-Tree implementation (both single threaded and parallel) with a 
// number of tunable parameters: size ratio between levels, storage layer used for each level (RAM, SSD, HDD), 
// different merging strategies, and any additional tuning parameters the students design. The final LSM-Tree 
// is expected to support high update throughput (in the order of 100K-1M updates per second for flash storage 
// and 1K-10K of updates per second for HDD storage), while at the same time provide efficient reads (in 
// the order of 1K-5K reads per second for flash storage and 20-100 reads per second for HDD storage). For 
// range queries, the performance of a short range query can ideally be close to point query performance 
// whereas the performance of a long range query depends on selectivity. However, the performance should 
// asymptotically be better than simply querying every key in the range using GET. The parallel LSM-Tree 
// is expected to scale with number of cores, that is, we expect to see that as the number of cores used 
// increases the performance of the tree is precipitously increasing.
// Testing Disk Speed

// For fwrite (option 2) result was 450-500 MB/sec. Best speed was at 8MB

// option1, 1MB: 4ms
// option1, 2MB: 7ms
// option1, 4MB: 14ms
// option1, 8MB: 28ms
// option1, 16MB: 59ms
// option1, 32MB: 71ms
// option1, 64MB: 132ms
// option1, 128MB: 291ms
// option1, 256MB: 567ms
// option1, 512MB: 1242ms
// option1, 1024MB: 2414ms
// option1, 2048MB: 5452ms
// option1, 4096MB: 11967ms
// option2, 1MB: 197ms
// option2, 2MB: 6ms
// option2, 4MB: 8ms
// option2, 8MB: 16ms
// option2, 16MB: 30ms
// option2, 32MB: 60ms
// option2, 64MB: 124ms
// option2, 128MB: 264ms
// option2, 256MB: 594ms
// option2, 512MB: 1035ms
// option2, 1024MB: 2154ms
// option2, 2048MB: 4499ms
// option2, 4096MB: 8844ms
// option3, 1MB: 67ms
// option3, 2MB: 5ms
// option3, 4MB: 14ms
// option3, 8MB: 17ms
// option3, 16MB: 33ms
// option3, 32MB: 66ms
// option3, 64MB: 130ms
// option3, 128MB: 276ms
// option3, 256MB: 583ms
// option3, 512MB: 1112ms
// option3, 1024MB: 2313ms
// option3, 2048MB: 4994ms
// option3, 4096MB: 10119ms



// M_buffer = P * B * E
// where B is the number of entries that fit into a disk page, P is the amount of main memory in terms of disk pages allocated to the buffer, and E is the average size of data entries

// In our dataset, all entries are of equal size i.e.

// 1 entry = (delete_bit, key, val) = int + int + 1 = 9 bytes
//  => E = 9
// B = page_size / entry_size = 4096 / 9 = 455
// P = 8 (for now)

// M_buffer = 8 * 8 * 512 = 32 KB


