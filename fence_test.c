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

static int filesize = 256;
static int SZ_RATIO = 10;

static struct fence* fences;

struct entry {
    int key;
    int val;
    int extra;
    bool del;
};


struct fence{
    int min;
    int max;
};


void build_fence(struct entry* data, int l, int r);
int search_fences(int key, int l, int r);

void build_fence(struct entry* data, int l, int r) {

    int step = PAGE_SZ / sizeof(struct entry);

    for(int i = 0; i < 256; i+= step) {
        fences[i].min = data[i].key;
        fences[i].max = data[i + step - 1].key;
    }
}


// binary search through fence pointers of a run. Return value of associated key if found, otherwise 0.
int search_fences(int key, int l, int r) {

    int left = 0;
    int right = ((sizeof(struct entry) * filesize * pow(SZ_RATIO, l)) / PAGE_SZ) - 1;

    while (left <= right) { 
        int m = left + (right - left) / 2; 

        if (key >= fences[m].min && 
            key <= fences[m].max) { 

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
        
        if (key > fences[m].max) {
            left = m + 1; 
        } else {
            right = m - 1; 
        }
    } 
  
    return 0; 
} 


int main(int argc, char* argv[]) {

    struct stat statbuf;
    // int out_size = filesize * sizeof(struct entry);
    int out = open("data/file_0_0.bin", O_RDONLY, S_IRUSR | S_IWUSR);
    // ftruncate(out, out_size);
  
    if (fstat (out,&statbuf) < 0) {
        printf ("fstat error");
        return 0;
    }

    struct entry* m_out = mmap(NULL, statbuf.st_size, PROT_READ, MAP_PRIVATE, out, 0);
    int num_keys = statbuf.st_size  / sizeof(struct entry);

    fences = (struct fence*) malloc(sizeof(struct fence) * ((sizeof(struct entry) * filesize) / PAGE_SZ) );
    build_fence(m_out, 0, 0);

    printf("thhhh %d %d\n", fences[0].min, fences[0].max);

    // search_fences(1880135773, 0,0);

    printf("value is %d\n\n", search_fences(1880135773, 0,0));
    free(fences);
    munmap(m_out, statbuf.st_size);
    close(out);
    return 0;
}



  
// A iterative binary search function. It returns 
// location of x in given array arr[l..r] if present, 
// otherwise -1 
// int binarySearch(int arr[], int l, int r, int x) { 
//     while (l <= r) { 
//         int m = l + (r - l) / 2; 
  
//         // Check if x is present at mid 
//         if (arr[m] == x) 
//             return m; 
  
//         // If x greater, ignore left half 
//         if (arr[m] < x) 
//             l = m + 1; 
  
//         // If x is smaller, ignore right half 
//         else
//             r = m - 1; 
//     } 
  
//     // if we reach here, then element was 
//     // not present 
//     return -1; 
// } 
  
// int main(void)  { 
//     int arr[] = { 2, 3, 4, 10, 40 }; 
//     int n = sizeof(arr) / sizeof(arr[0]); 
//     int x = 10; 
//     int result = binarySearch(arr, 0, n - 1, x); 
//     (result == -1) ? printf("Element is not present"
//                             " in array") 
//                    : printf("Element is present at "
//                             "index %d", 
//                             result); 
//     return 0; 
// } 


