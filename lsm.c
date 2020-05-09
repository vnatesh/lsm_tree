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
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/time.h> 
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/select.h>
#include <fcntl.h>

#include "threadpool.h"
// #include "MurmurHash3.h"

#include "xxh3.h"


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
    int extra;
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
};

struct run {
    struct fence* fences;
    char* file;
    char* bloom;
    // int bits_per_entry;
    // int num_hash;
};

struct level {
    struct run* runs;
    int run_cnt;
    int bits_per_entry; // # of bits per entry in bloom filters of this level
    int num_hash;
    int entries_per_run; // # of entries per run. This is the max since some runs will have less due to deletes/updates
    enum policy pol;
};

// file_pair for binary merge
struct file_pair {
    char f1[32];
    char f2[32];
    char out_f[32];
    int group;
    int f1_size;
    int f2_size;
};

// keyval type for load command .bin files
struct keyval {
    int key;
    int val;
};

// tunable user inputs (knobs)
static int SZ_RATIO;
static unsigned long long int M_BUFFER;
static char* static_workload = NULL;
static int NUM_CLIENTS;
static unsigned long long int MAX_MEM;
static int L1_BPE;


static const char delims[] = " ";
static struct buffer* L0;
static struct level* levels;
static int NUM_LEVELS;

static threadpool_t* pool; // global thread pool
static char *socket_path = "ipc_socket";

// global vars for merge threads
static int g_cnt = 0; // number of merge groups (# of 1's in bin(SZ_RATIO))
static int* groups; // file ranges for each group (start,end)
static int group_wait = 0;
static int* k_wait = NULL;
static int curr_merge_lev;
static int merge_filesize;

// locks
static pthread_mutex_t group_lock;
static pthread_mutex_t* k_locks;
static pthread_rwlock_t rwlock;
static pthread_mutex_t level_table_lock;


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
void build_filter(struct entry* data, int l, int r);
void bloom_set(int key, int l, int r);
bool bloom_test(int key, int l, int r);

// k-way merge functions
void merge_and_flush(void* p);
void launch_merge();
struct file_pair* k_way_merge(int start, int end, int g_id);
void binary_merge(void* inp);

// locks
void locks_init();
void locks_destroy();

// fence pointer functions
void build_fences(struct entry* data, int l, int r);
int search_fences(int key, int l, int r);


// reads
int get(int key);

// level management
void levels_init();
void levels_free();
void levels_persist();




void locks_init() { 

    // read-write lock to manage access to the levels metadata
    if (pthread_rwlock_init(&rwlock, NULL) != 0) { 
        printf("\nError: rwlock init failed\n"); 
        exit(1); 
    } 

    // mutex lock to manage access to the levels metadata
    if (pthread_mutex_init(&level_table_lock, NULL) != 0) { 
        printf("\nError: mutex init failed\n"); 
        return ; 
    } 

    int n = SZ_RATIO;
    // convert n to binary, find position where val is 1 and create group of size 2^position
    for(int i = 0; n > 0; i += 2) {
        if(n % 2) {
            g_cnt++;
        } 
        n /= 2;
    }

    k_wait = (int*) malloc(sizeof(int) * g_cnt);
    k_locks = (pthread_mutex_t*) malloc(sizeof(pthread_mutex_t) * g_cnt);

    for(int i = 0; i < g_cnt; i++) {
        if (pthread_mutex_init(&k_locks[i], NULL) != 0) { 
            printf("\nError: mutex init failed\n"); 
            return ; 
        }  
    }
}

// This function initializes metadata for each level in the LSM tree according to 
// user defined policies (leveling, tiering, lazy leveling). It also implements the 
// Monkey optimizations for bloom filters
// TODO : need to support leveling, tiering, or lazy-leveling policies for merge in each level
void levels_init() {
    // L = ln((N*E / M_BUF) * ((T-1) / T)) / ln(T)
    NUM_LEVELS = (int) ceil(log((((double) MAX_MEM) / ((double) M_BUFFER)) * 
                (((double) (SZ_RATIO-1)) / ((double) SZ_RATIO))) / log(SZ_RATIO));

    levels = (struct level*) malloc(sizeof(struct level) * NUM_LEVELS);

    // user supplies bits per entry for level 1 (L1_BPE)
    double p1 = exp(-L1_BPE * pow(log(2),2));
    // get the # of levels for which we assign bloom filters. All levels >= this one will not be given any filters
    int filtered_levels =  (int) floor((((double) L1_BPE) * pow(log(2),2)) / (log(SZ_RATIO)));
    int bits_per_entry;
    int entries_per_run;
    int num_hash;
    // int extra;

    // below loop is just for a tiering policy, allocates SZ_RATIO runs
    // TODO : make this a separate function and create another function for the leveling policy in a run. 
    // User should indicate what type of policy to use in each level (maybe via some kind of struct). 
    for(int i = 0; i < NUM_LEVELS; i++) {

        bits_per_entry = (int) ceil(-log(pow(SZ_RATIO, i) * p1) / pow(log(2),2));
        entries_per_run = (int) (L0->length * pow(SZ_RATIO, i));
        num_hash = (int) ceil(((double) bits_per_entry) * log(2));

        levels[i].bits_per_entry = bits_per_entry;
        levels[i].num_hash = num_hash;
        levels[i].runs = (struct run*) malloc(sizeof(struct run) * SZ_RATIO);
        levels[i].run_cnt = 0;
        levels[i].entries_per_run = entries_per_run;

        if(i < filtered_levels) {
            for(int j = 0; j < SZ_RATIO; j++) {
                // allocate and initialize bloom_f to zeroes
                levels[i].runs[j].bloom = (char*) calloc((entries_per_run * bits_per_entry) / 8, 1);
                levels[i].runs[j].fences = (struct fence*) malloc(sizeof(struct fence) * 
                                    ((sizeof(struct entry) * entries_per_run) / PAGE_SZ) );
            }
        } else {
            for(int j = 0; j < SZ_RATIO; j++) {

                levels[i].runs[j].bloom = NULL; // no allocation of bloom_f for unfiltered levels
                levels[i].runs[j].fences = (struct fence*) malloc(sizeof(struct fence) * 
                                    ((sizeof(struct entry) * entries_per_run) / PAGE_SZ) );
            }
        }
    }
}


// From "Less Hashing, Same Performance: Building a Better Bloom Filter Adam Kirsch, Michael Mitzenmacher"
// http://citeseerx.ist.psu.edu/viewdoc/download;jsessionid=95B8140B9772E2DDB24199DA24DE5DB1?doi=10.1.1.152.579&rep=rep1&type=pdf
// gi(x) = h1(x) + ih2(x)
void bloom_set(int key, int l, int r) {

    // uint64_t out[2];
    int h = levels[l].num_hash;
    int n = levels[l].entries_per_run;
    int b = levels[l].bits_per_entry;

    // MurmurHash3_x64_128(&key, sizeof(int), 0, &out);

    // XXH64_hash_t out = XXH3(&key, sizeof(int), 0);
    XXH128_hash_t out = XXH128(&key, sizeof(int), 0);


    for(int i = 0; i < h; i++) {
        // SetBit(levels[l].runs[r].bloom, ((out[0] + i * out[1]) % (n * b)));
        SetBit(levels[l].runs[r].bloom, ((out.low64 + i * out.high64) % (n * b)));
    } 
}


bool bloom_test(int key, int l, int r) {

    // uint64_t out[2];
    int h = levels[l].num_hash;
    int n = levels[l].entries_per_run;
    int b = levels[l].bits_per_entry;

    // MurmurHash3_x64_128(&key, sizeof(int), 0, &out);
    XXH128_hash_t out = XXH128(&key, sizeof(int), 0);
    for(int i = 0; i < h; i++) {
        // if(!TestBit(levels[l].runs[r].bloom, ((out[0] + i * out[1]) % (n * b)))) {
        if(!TestBit(levels[l].runs[r].bloom, ((out.low64 + i * out.high64) % (n * b)))) {

            return false;
        }
    }

    return true;
}


void build_filter(struct entry* data, int l, int r) {

    int n = levels[l].entries_per_run;
    int b = levels[l].bits_per_entry;
    // reset memory to zeroes in case the filter has been used before
    memset(levels[l].runs[r].bloom, 0, ((n * b) / 8));

    for(int i = 0; i < n; i++) {
         bloom_set(data[i].key, l, r);
    }
}

    // double x = ceil(bits_per_entry * log(2));
    // x = x + 0.5 - (x<0); // x is now 55.499999...
    // int num_hash = (int) x; // truncated to 55
    // // printf("num_hashes%d\n", num_hash);
    // int* arr = (int*) malloc(sizeof(int) * num_entries);
    // for(int i = 0; i < num_entries; i++) {
    //     arr[i] = i;
    // }

    // char* bloom = (char*) malloc((num_entries * bits_per_entry) / 8); 
    // build_filter(arr, bloom, num_hash);


    // srand(time(NULL));
    // int fp_cnt = 0;
    // int a;
    // for(int i = 0; i < num_entries; i++) {
    //     a = rand();
    //     if(bloom_test(bloom, a, num_hash)) {
    //         if(a >= num_entries) {
    //             printf("FP : %d\n", a);
    //             fp_cnt++;
    //         }
    //     }
    // }

    // printf("\n\n FPR : %.9lf\n", ((double) fp_cnt) / ((double) num_entries));

void build_fences(struct entry* data, int l, int r) {

    int step = PAGE_SZ / sizeof(struct entry);

    for(int i = 0; i < ((sizeof(struct entry) * levels[l].entries_per_run) / PAGE_SZ); i++) {
        levels[l].runs[r].fences[i].min = data[i*step].key;
        levels[l].runs[r].fences[i].max = data[i*step + step - 1].key;
    }
    // printf("%d %d\n", levels[l].runs[r].fences[200].min, levels[l].runs[r].fences[200].max);

}


// binary search through fence pointers of a run. Return value of associated key if found, otherwise 0.
// TODO : the last fence is wrong since it wont be at a perfect offset of 4096. 
// TODO : try linear search, see if its faster
int search_fences(int key, int l, int r) {

    int left = 0;
    int right = ((sizeof(struct entry) * L0->length * pow(SZ_RATIO, l)) / PAGE_SZ) - 1;

    while (left <= right) { 

        int m = left + (right - left) / 2; 

        if (key >= levels[l].runs[r].fences[m].min && 
            key <= levels[l].runs[r].fences[m].max) { 

            char name[32];
            sprintf(name, "data/file_%d_%d.bin", l, r);
            int fd = open(name, O_RDONLY, S_IRUSR | S_IWUSR);
            struct entry* page = mmap(NULL, PAGE_SZ, PROT_READ, MAP_PRIVATE, fd, m * PAGE_SZ);
            int left_pg = 0;
            int right_pg = (PAGE_SZ / sizeof(struct entry)) - 1;

            // binary search through the page
            while (left_pg <= right_pg) { 

                int mid = left_pg + (right_pg - left_pg) / 2; 

                if (page[mid].key == key) {
                    int v = page[mid].val;
                    munmap(page, PAGE_SZ);
                    close(fd);
                    return v; 
                }
          
                if (page[mid].key < key) {
                    left_pg = mid + 1; 
                } else {
                    right_pg = mid - 1; 
                }
            } 

            munmap(page, PAGE_SZ);
            close(fd);
            return 0; 
        } 
        
        if (key > levels[l].runs[r].fences[m].max) {
            left = m + 1; 
        } else {
            right = m - 1; 
        }
    } 
  
    return 0; 
} 


// write-optimized buffer...insert O(1) (average case), delete O(lgn), but reads (search) is O(n)
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



static int flush_cnt = 0;
// TODO : make sure you don't flush the buffer while a merge is happening on
// level 0. Level 0 will be full so any flushing will overwrite
void buffer_flush() {
    // Wait if level 0 is full. Needs to be merged and flushed so levels[0]->run_cnt will == 0.
    while(levels[0].run_cnt == SZ_RATIO) {}

    int r = levels[0].run_cnt;
    char filename[32];
    sprintf(filename, "data/file_%d_%d.bin", 0, levels[0].run_cnt);

    FILE* fp = fopen(filename , "wb");
    if(!fp) {
        printf("Error: Could not open file\n");
        exit(1);
    }

    qsort((void*) (L0->data + 1), (size_t) L0->heap_size, sizeof(struct entry), comparator);
    fwrite(L0->data + 1, sizeof(struct entry), L0->length, fp);
    fclose(fp);

    // build filter/fence for new runs that aren't the last one (SZ_RATIO'th run, since that run would be merged/flushed anyway)
    if(r != SZ_RATIO - 1) {
        build_filter(L0->data + 1, 0, r);
        build_fences(L0->data + 1, 0, r);
    }

    pthread_rwlock_wrlock(&rwlock);
    // pthread_mutex_lock(&level_table_lock); 
    levels[0].run_cnt++;
    // pthread_mutex_unlock(&level_table_lock); 
    pthread_rwlock_unlock(&rwlock);

    L0->heap_size = 0;
    curr_merge_lev = 0;
    // // launch merge/flush on separate thread so it happens in background
    // threadpool_add(pool, merge_and_flush, (void*) &curr_merge_lev , 1);
    merge_and_flush((void*) &curr_merge_lev);
}


void merge_and_flush(void* p) {

    // cascading flush to disk if num_files in level i == SZ_RATIO
    while(1) {
        if(levels[curr_merge_lev].run_cnt == SZ_RATIO) {
            flush_cnt++;
            merge_filesize = (int) (L0->length * pow(SZ_RATIO, curr_merge_lev));
            launch_merge();
            curr_merge_lev++;
        } else {
            break;
        }
    }
}


void launch_merge() {
    // group_len is the maximum number of bits i.e. groups we would need to track
    // g_cnt is the actual number of groups that we track i.e. where bit is 1
    int group_len = (int) (floor(log2(SZ_RATIO)) + 1);
    group_wait = 0;
    int n = SZ_RATIO;
    groups = (int*) malloc(sizeof(int) * group_len * 2);
    int curr = 0;

    // convert n to binary, find position where val is 1 and create group of size 2^position
    for(int i = 0; n > 0; i += 2) {

        if(n % 2) {
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

    // We start the merge process by starting work (merge) on the larger groups (size determined by MSB of SZ_RATIO in binary)
    // first so that the smaller groups can be merging simulatenously and possibly finish before the large group. Small group
    // threads can be running while large group is running
    int x = 0;
    struct file_pair* outs[g_cnt+1]; 

    for(int i = (group_len*2) - 1; i > 0; i -= 2) {
        if(groups[i] != -1) {
            outs[x] = k_way_merge(groups[i-1], groups[i], x);
            x++;
        }
    }

    while(group_wait != 0) {}

    // int level = 3;
    // int run = 4;
    int l = curr_merge_lev;
    int r = levels[l+1].run_cnt;
    struct file_pair output;
    char newname[32];

    // Since SZ_RATIO is maxed at 10, at most 3 groups will be present (3 out of 4 bits turned on).
    if(g_cnt == 1) {
        sprintf(newname, "data/file_%d_%d.bin", l+1, r);
        rename(outs[0]->out_f, newname);
    
    } else if(g_cnt == 2) {

        strcpy(output.f1, outs[1]->out_f);
        strcpy(output.f2, outs[0]->out_f);
        output.group = 0;
        output.f1_size = outs[1]->f1_size + outs[1]->f2_size;
        output.f2_size = outs[0]->f1_size + outs[0]->f2_size;
        sprintf(output.out_f, "data/file_%d_%d.bin", l+1, r);
        binary_merge((void*) &output);
    
    } else if(g_cnt == 3) {

        strcpy(output.f1, outs[2]->out_f);
        strcpy(output.f2, outs[1]->out_f);
        output.group = 0;
        output.f1_size = outs[2]->f1_size + outs[2]->f2_size;
        output.f2_size = outs[1]->f1_size + outs[1]->f2_size;
        sprintf(output.out_f, "merge/intermediate");
        binary_merge((void*) &output);

        sprintf(output.f1, "merge/intermediate");
        strcpy(output.f2, outs[0]->out_f);
        output.group = 0;
        output.f1_size = outs[2]->f1_size + outs[2]->f2_size;
        output.f2_size = outs[0]->f1_size + outs[0]->f2_size;
        sprintf(output.out_f, "data/file_%d_%d.bin", l+1, r);
        binary_merge((void*) &output);
    }

    // Sweep through the final merged file and create the bloom
    // filters (only for filtered levels from Monkey) and fence pointers for this new run
    int out = open(output.out_f, O_RDONLY, S_IRUSR | S_IWUSR);
    struct entry* m_out = mmap(NULL, levels[l+1].entries_per_run * sizeof(struct entry), 
                                PROT_READ, MAP_PRIVATE, out, 0);
    if(levels[l+1].runs[r].bloom != NULL) {
        build_filter(m_out, l+1, r);
    }    
    build_fences(m_out, l+1, r);

    // rwlock write lock the levels table, and delete all old files and intermediate files, update
    // levels metadata so readers see a consistent snapshot of the data
    // TODO : only lock
    pthread_rwlock_wrlock(&rwlock);
    levels[l+1].run_cnt++;
    levels[l].run_cnt = 0;
    
    // delete old files 
    for(int i = 0; i < SZ_RATIO; i++) {
        sprintf(newname, "data/file_%d_%d.bin", l, i);
        if(remove(newname)) {
            printf("Failed to delete file"); 
        }
    }

    pthread_rwlock_unlock(&rwlock);

    munmap(m_out, levels[l+1].entries_per_run * sizeof(struct entry));
    close(out);

    if(g_cnt == 3) {
        if(remove("merge/intermediate")) {
            printf("Failed to delete file"); 
        }
    }

    char cmd[32];
    strcpy(cmd, "rm merge/out_*" );
    system(cmd);
}


// Merge k = (end-start) files together in parallel. k (a) is guaranteed to be a power of 2.
// Launch a binary merge on each of log2(k) threads in parallel
struct file_pair* k_way_merge(int start, int end, int g_id) {

    int k = end - start;
    if(k < 2) {
        // do nothing, you need at least k=2 files to merge
        struct file_pair* a = (struct file_pair*) malloc(sizeof(struct file_pair) * 1);
        sprintf(a->out_f, "data/file_%d_%d.bin", curr_merge_lev, start);
        a->group = g_id;

        return a;
    }

    int ind = 0;
    int p_ind = 0;

    struct file_pair** q = (struct file_pair**) malloc(sizeof(struct file_pair*) * ((int) log2(k)));
    q[ind] = (struct file_pair*) malloc(sizeof(struct file_pair) * (k/2));

    for(int i = start; i < end; i+=2) {
        sprintf(q[ind][p_ind].f1, "data/file_%d_%d.bin", curr_merge_lev, i);
        sprintf(q[ind][p_ind].f2, "data/file_%d_%d.bin", curr_merge_lev, i+1);
        sprintf(q[ind][p_ind].out_f, "merge/out_%d_%d.bin", i+1, ind);
        q[ind][p_ind].group = g_id;

        q[ind][p_ind].f1_size = merge_filesize;
        q[ind][p_ind].f2_size = merge_filesize;

        p_ind++;
    } 

    ind++;
    int j = 2;
    
    while(ind < ((int) log2(k))) {

        p_ind = 0;
        q[ind] = (struct file_pair*) malloc(sizeof(struct file_pair) * (k/2));

        for(int i = j; i < k; i += 2*j) {
            sprintf(q[ind][p_ind].f1, "merge/out_%d_%d.bin", i + start - 1, ind - 1);
            sprintf(q[ind][p_ind].f2, "merge/out_%d_%d.bin", i + start + j - 1, ind - 1);
            sprintf(q[ind][p_ind].out_f, "merge/out_%d_%d.bin", i + start + j - 1, ind);
            q[ind][p_ind].group = g_id;

            q[ind][p_ind].f1_size = merge_filesize * j;
            q[ind][p_ind].f2_size = merge_filesize * j;

            p_ind++;
        }

        ind++;
        j *= 2;
    }
 
    for(int i = 0; i < ((int) log2(k)); i++) {
        k_wait[g_id] = (int) pow(2, (((int) log2(k)) - i - 1));
        for(int j = 0; j < (k/2); j++) {
            if(strlen(q[i][j].f1) != 0) {
                threadpool_add(pool, binary_merge, (void*) &q[i][j] , 1);
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

    munmap(m1, inp_size1);
    close(fd1);
    munmap(m2, inp_size2);
    close(fd2);
    munmap(m_out, out_size);
    close(out);

    pthread_mutex_lock(&k_locks[f_pair.group]); 
    k_wait[f_pair.group]--;
    pthread_mutex_unlock(&k_locks[f_pair.group]); 
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
        for(int j = levels[i].run_cnt - 1; j >= 0; j--) { // Go to most recent run first since it may contain updated keys
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
                    get(q.inp1);
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
                M_BUFFER = strtoull(optarg, NULL, 10) * (1 << 10); // convert KB to bytes
                break;

            case 'f':
                static_workload = optarg;
                break;

            case 'c':
                NUM_CLIENTS = atoi(optarg);
                break;

            case 'd':
                MAX_MEM = strtoull(optarg, NULL, 10) * (1 << 20); // convert MB to bytes
                break;

            case 'e':
                L1_BPE = atoi(optarg); // bits per entry in L1
                break;

            case '?':
                if (optopt == 'r' || optopt == 'b' || optopt == 'f' || optopt == 'c'
                    || optopt == 'd' || optopt == 'e') {
                    fprintf(stderr, "Option -%c requires an argument.\n", optopt);
                    fprintf(stderr, "-r : size ratio\n");
                    fprintf(stderr, "-b : buffer size (KB)\n");
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

    //  If there is already data in the data directory, then load 
    // in the auxilliary bloom/fence data as well as the buffer, otherwise run levels_init
    // startup():

    buffer_init();
    locks_init();
    levels_init();
    pool = threadpool_create(MAX_THREADS, MAX_QUEUE, 0);
    run_server();

    if(static_workload) {

        gettimeofday (&start, NULL);

        buffer_load(static_workload);

        printf("heap size : %d\n", L0->heap_size);
        gettimeofday (&end, NULL);
        diff_t = (((end.tv_sec - start.tv_sec)*1000000L
            +end.tv_usec) - start.tv_usec) / (1000000.0);
        printf("new buffer load time: %f\n", diff_t); 
        // printf("\n\n\n\n");
        // for(int i = 1; i <= L0->heap_size; i++) {
        //     printf("%d : %d : %d\n",L0->data[i].del, L0->data[i].key, L0->data[i].val);
        // }


        // for(int i = 1; i <= L0->heap_size; i++) {
        //     printf("%d : %d : %d\n",L0->data[i].del, L0->data[i].key, L0->data[i].val);
        // }
    }


    gettimeofday (&start, NULL);


    char line[31];
    FILE *fp = fopen("test_wkload_32M.txt", "r");
    char* a;

    struct query q;

    while(fgets(line, sizeof(line), fp) != NULL) {

        if(line[0] != '\n') {

            a = strtok(line, delims);

            switch(a[0]) {
                case 'p': 
                    q.type = PUT;
                    q.inp1 = atoi(strtok(NULL, delims));
                    q.inp2 = atoi(strtok(NULL, delims));
                    printf("%d  %d\n",q.inp1,get(q.inp1));
                    break;
                default: 
                    printf("Error : Invalid Command"); 
                    exit(1);
            }   
        }
    }

    fclose(fp);

    // printf("get is %d\n",get(730962444));
    gettimeofday (&end, NULL);
    diff_t = (((end.tv_sec - start.tv_sec)*1000000L
        +end.tv_usec) - start.tv_usec) / (1000000.0);
    printf("Read time: %f\n", diff_t); 


    buffer_free();
    locks_destroy();
    levels_free();
    threadpool_destroy(pool, 1);

    // sleep(100);
    return 0;
}


void run_server() {

    struct sockaddr_un addr;
    int client_fd;
    int socket_fd;

    if ( (socket_fd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
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
        unlink(socket_path);
    }

    if (bind(socket_fd, (struct sockaddr*) &addr, sizeof(addr)) == -1) {
        perror("bind error");
        exit(-1);
    }

    // TODO : experiment with backlog (queue) value...change from 5 to 10 or 32...
    if (listen(socket_fd, 5) == -1) {
        perror("listen error");
        exit(-1);
    }

    while (1) {
        if ( (client_fd = accept(socket_fd, NULL, NULL)) == -1) {
            perror("accept error");
            continue;
        }

        threadpool_add(pool, client_handler, (void*) &client_fd , 1);
    }

    return 0;
}



// load queries statically from a file
// TODO : change buffer_load to this function to handle load command .bin files
void buffer_load(char* filename) {

    struct query q;
    struct stat statbuf;
    int fd = open(filename, O_RDONLY, S_IRUSR | S_IWUSR);
  
    if (fstat (fd, &statbuf) < 0) {
        printf ("fstat error");
        return 0;
    }

    struct keyval* m_out = mmap(NULL, statbuf.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    int num_entries = statbuf.st_size  / sizeof(struct keyval);

    for(int i = 0; i < num_entries; i++) {

        if(L0->heap_size == L0->length) {
            buffer_flush();  
        } else {
            q.type = PUT;
            q.inp1 = m_out[i].val;
            q.inp2 = m_out[i].val;
            buffer_insert(q);
        }        
    }

    munmap(m_out, statbuf.st_size);
    close(out)
}


void client_handler(void* client) {

    int client_fd = *((int*) client);
    int rc;
    char line[31];
    char* a;
    struct query q;

    char val[15];
    // char* buf = (char*) malloc(sizeof(struct entry) * );
    // while ((rc = read(client_fd,buf, sizeof(buf))) > 0) {
    //     printf("read %u bytes: %.*s\n", rc, rc, buf);
    // }

    while ((rc = read(client_fd, line, sizeof(line))) > 0) {

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
                    pthread_rwlock_wrlock(&rwlock);
                    buffer_insert(q);
                    pthread_rwlock_unlock(&rwlock);
                    break;
                case 'd': 
                    q.type = DELETE;
                    q.inp1 = atoi(strtok(NULL, delims));
                    pthread_rwlock_wrlock(&rwlock);
                    buffer_insert(q);
                    pthread_rwlock_unlock(&rwlock);
                    break;
                case 'g': 
                    q.type = GET;
                    q.inp1 = atoi(strtok(NULL, delims));
                    pthread_rwlock_rdlock(&rwlock);
                    itoa(get(q.inp1), val, 10);
                    pthread_rwlock_unlock(&rwlock);
                    write(client_fd, val, nread);
                    break; 
                case 'r': 
                    q.type = RANGE;
                    q.inp1 = atoi(strtok(NULL, delims));
                    q.inp2 = atoi(strtok(NULL, delims));
                    pthread_rwlock_rdlock(&rwlock);
                    // range() // TODO : range query
                    pthread_rwlock_unlock(&rwlock);
                    break; 
                // case 's':
                case 'l':
                    buffer_load(static_workload);
                    break;
                default: 
                    printf("Error : Invalid Command"); 
                    exit(1);
            }   
        }
    }

    close(client_fd);
}


// Save the bloom filters and fence pointer arrays in each level to disk
// Also persist the buffer to disk
void levels_persist() {

    char filename[32];
    sprintf(filename, "data/buffer.bin");

    FILE* fp = fopen(filename , "wb");
    if(!fp) {
        printf("Error: Could not open file\n");
        exit(1);
    }

    fwrite(L0->data + 1, sizeof(struct entry), L0->length, fp);
    fclose(fp);

    // if not found in buffer, search LSM tree on disk
    for(int i = 0; i < NUM_LEVELS; i++) {
        for(int j = 0; j < levels[i].run_cnt; j++) { // Go to most recent run first since it may contain updated keys

            sprintf(filename, "data/bloom/file_%d_%d.bin", i, j);
            fp = fopen(filename , "wb");
            if(!fp) {
                printf("Error: Could not open file\n");
                exit(1);
            }
            fwrite(levels[i].runs[j].bloom , 1, 
                    (levels[i].entries_per_run * levels[i].bits_per_entry) / 8, fp);
            fclose(fp);

            sprintf(filename, "data/fence/file_%d_%d.bin", i, j);
            fp = fopen(filename , "wb");
            if(!fp) {
                printf("Error: Could not open file\n");
                exit(1);
            }
            fwrite(levels[i].runs[j].fences , sizeof(struct fence), 
                    (sizeof(struct entry) * levels[i].entries_per_run) / PAGE_SZ, fp);
            fclose(fp);
        }
    }
}


void levels_free() {
    for(int i = 0; i < NUM_LEVELS; i++) {
        for(int j = 0; j < SZ_RATIO; j++) {
            free(levels[i].runs[j].bloom);
            free(levels[i].runs[j].fences);
        }
        free(levels[i].runs);
    }
    free(levels);
}


void buffer_free() {
    free(L0->data);
    free(L0);
}


void locks_destroy() {

    for(int i = 0; i < g_cnt; i++) {
        pthread_mutex_destroy(&k_locks[i]); 
    }

    pthread_mutex_destroy(&level_table_lock); 
    pthread_mutex_destroy(&group_lock); 
    pthread_rwlock_destroy(&rwlock);
    free(k_locks);
    free(k_wait);
    free(groups);
}

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


// // Future optimizations:

// Currently, the entry size is 16 bytes instead of 8 bytes due to the delete bit and the extra bits fegnrelghjrjhgirgkrjgiorjgioerjgoir
// allocated for alignment. However, in the future it would be better to only store the delete bit in the 
// first level on disk. The sort/merge/delete process will ensure that all greater levels only need the key and
// val, allowing an entry in these levels to only need 8 bytes. This would reduce the total memory by ~ 50% 
// and improve read/merge throughput. 


// A parallel k-way merge algorithm was used to improve merge performance. In this algorithm, the merging of files within a level
// happens in parallel 

// cd cs265-sysproj/generator/; 
// ./generator --puts 20000 --gets 100 --ranges 100 --deletes 1--gets-misses-ratio 0.1 --gets-skewness 0.2 > b.txt; 
// mv b.txt ../..; 
// cd ../..;

// gcc -Wall -g -O0 -pthread lsm.c threadpool.c MurmurHash3.c -o lsm
// ./lsm -r10 -b4 -ftest_wkload.txt -c1 -d100 -e10

// ./lsm -r10 -b1024 -ftest_wkload_32M.txt -c1 -d100 -e10

// ./generator --puts 2000000 --gets-misses-ratio 0.1 --gets-skewness 0.2 > test_wkload_32M.txt; 
// mv test_wkload_32M.txt ../..; 

// ./generator --puts 20000000 --gets-misses-ratio 0.1 --gets-skewness 0.2 > test_wkload_320M.txt;
// mv test_wkload_320M.txt ../..; 

// ./lsm -r10 -b4096 -ftest_wkload_320M.txt -c1 -d1000 -e10



// gcc -Wall -g -O0 -pthread lsm.c threadpool.c -o lsm
// ./lsm -r10 -b1024 -ftest_wkload_32M.txt -c1 -d100 -e10


// valgrind --leak-check=yes --track-origins=yes ./lsm -r10 -b4 -ftest_wkload.txt -c1 -d100 -e10

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


// Need to use a tiering policy for all levels except the last level where we use leveling (1 run Dotstoevsky)  

// Do locking per level or per run...If we do 1 lock per run, then the
// number of locks is size_ratio * lg(n) (n is the number of entries)
// follow cocurrent clocking scheme in https://www.cs.rochester.edu/u/scott/papers/1996_IPL_heaps.pdf

// updates and deletes are done on merging of the levels. They are not done in the buffer.

// We can use     pthread_rwlock_wrlock(&rwlock); 
// Before deleting unecessary read files, it will check if the rw_lock is being held by any reader threads (pthread_rwlock_rdlock(&rwlock)). 
// Wait until lock acquired. When we get the lock, delete files that had existed before merge

// How do we track disk pages in the database (for fence pointers) if malloc can create blocks of memory that straddle several pages?

// ./generator --puts 200000 --gets 100 --ranges 100 --deletes 10 --gets-misses-ratio 0.1 --gets-skewness 0.2 | sort -R > test.txt

// sudo sysctl -a | grep cache

// exit_func():
//  when user wants to exit, flush bloom filters and fence points to disk as a file in some defined format


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
// where B is the number of entries that fit into a disk page, P is the amount of main memory in terms of disk
// pages allocated to the buffer, and E is the average size of data entries

// In our dataset, all entries are of equal size i.e.

// 1 entry = (delete_bit, key, val) = int + int + 1 = 9 bytes
//  => E = 9
// B = page_size / entry_size = 4096 / 9 = 455
// P = 8 (for now)

// M_buffer = 8 * 8 * 512 = 32 KB









// static int merge_filesize = 10000000;

// merge experiments...shows that parallel k-way merge is clearly faster than sequential

// VNatesh:project vikas$ gcc -g  -pthread merge_test.c threadpool.c -o merge_test
// VNatesh:project vikas$ ./merge_test
// merge time : 0.848140
// VNatesh:project vikas$ cd data/
// VNatesh:data vikas$ rm file_*
// VNatesh:data vikas$ rm out_*
// VNatesh:data vikas$ cd ..
// VNatesh:project vikas$ gcc -g  -pthread merge_test.c threadpool.c -o merge_test
// VNatesh:project vikas$ ./merge_test
// merge time : 2.162473



// VNatesh:project vikas$ gcc -g  -pthread merge_test.c threadpool.c -o merge_test
// VNatesh:project vikas$ ./merge_test
// merge time : 2.090853
// VNatesh:project vikas$ gcc -g  -pthread merge_test.c threadpool.c -o merge_test
// VNatesh:project vikas$ ./merge_test
// merge time : 0.754781
// VNatesh:project vikas$ cd data/


// M_BUFFER + (12 - M_BUFFER%12)


// The final report will be due on May 11 at noon (grades are due on May 12). Submission by PDF on Canvas. 
// (due date was May 9 but we are moving it to May 11).

// For your final reports, you should follow the instructions and template/format of the midway checkin report. 

// Systems projects meetings will be focused 50% on code review and 50% on discussion on optimization and analysis 
// of experimental results. 










// The following are the deliverables for midway check-in:

 

// Report: Your report should describe in detail the intended design of the first phase of the project, i.e, 
// you do not have to include concurrent execution. You may start with an overview of the design considerations 
// that you’ve made so far. For each design decision, you may specify (a) the intuition behind decision in the 
// first place, (b) a description of the design elements involved, and c) for the parts that are already 
// implemented you should describe the implementation at a high level. ​Additionally, you can include deeper
//  details with supporting materials such as, architecture diagrams, flowcharts, and/or pseudocode for 
//  clarification. This is optional for the midway check-in but including such graphics will allow us to 
//  give better feedback that will help you with the final report. 

 

// The report should contain at least two performance experiments (e.g., reporting response time) that 
// demonstrate an unoptimized variant of a get and a put operation. For e.g., the get operator does not
//  need to  have the bloom filter optimization proposed in Monkey. Ideally for each performance graph 
//  you can already have an additional graph that helps explain the results (e.g., you can count the 
//     number of I/O operations). 

 

// The document should be a maximum of 3 pages organized using the same template as of the final report 
// (but a shorter version): http://daslab.seas.harvard.edu/classes/cs265/files/Final_Report_Template.pdf. 
// You should format the document using a 10pt font, 1 inch margin using the latex template that can be
//  found at the ACM website: https://www.acm.org/publications/proceedings-template. You should submit a pdf through canvas. 

 

// This report will effectively be the starting point for your final end-of-semester report. 

 

// The report for the midway check-in will be due on March 12. There will be an entry on Canvas to submit 
// a PDF. This will give you some time to fine-tune your report after getting some feedback during your 
// code review meeting. 

 

// Code Review: Each student should sign up for a code review meeting here. Your goal for the code r
// eview meeting is to give a demo of your project so far, and in particular of the two working 
// operations. It is an opportunity to get feedback on both the design and the structure of your 
// code. During the meeting we will also “debug” your performance graphs. That is, you should be 
// ready to reproduce them, run variations of your scripts, and explain the results. This is an 
// opportunity for us to give you feedback on how to do good performance analysis graphs so you
//  can obtain good standards for your final report. 






