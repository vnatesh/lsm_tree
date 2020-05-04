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


#define SetBit(A,k)     ( A[(k/8)] |= (1 << (k%8)) )
#define ClearBit(A,k)   ( A[(k/8)] &= ~(1 << (k%8)) )            
#define TestBit(A,k)    ( A[(k/8)] & (1 << (k%8)) )

static int num_entries = 116883;
static int bits_per_entry = 10; // bits per entry to use on level 1. This should be user input
							// From this user input, we can compute the bits_per_entry for the bloom 
							// filters in the rest of the levels (ceil(-ln(T^i * p_i) / (ln(2)^2)))

void build_filter(int* arr, char* bloom, int num_hash);
void bloom_set(char* bloom, int key, int num_hash);
bool bloom_test(char* bloom, int key, int num_hash);


struct entry {
    int key;
    int val;
    bool del;
};

// from Less Hashing, Same Performance: Building a Better Bloom Filter Adam Kirsch, Michael Mitzenmacher
// gi(x) = h1(x) + ih2(x)
void bloom_set(char* bloom, int key, int num_hash) {

	uint64_t out[2];
	MurmurHash3_x64_128(&key, sizeof(int), 0, &out);

	for(int i = 0; i < num_hash; i++) {
		SetBit(bloom, ((out[0] + i * out[1]) % (num_entries * bits_per_entry)));
	} 
}

bool bloom_test(char* bloom, int key, int num_hash) {

	uint64_t out[2];
	MurmurHash3_x64_128(&key, sizeof(int), 0, &out);

	for(int i = 0; i < num_hash; i++) {
		if(!TestBit(bloom, ((out[0] + i * out[1]) % (num_entries * bits_per_entry)))) {
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

int main(int argc, char* argv[]) {

	printf("YOOO %lu\n", sizeof(struct entry));
	exit(1);

	double x = ceil(bits_per_entry * log(2));
	x = x + 0.5 - (x<0); // x is now 55.499999...
	int num_hash = (int) x; // truncated to 55
	printf("num_hashes %d\n", num_hash);

	unsigned long long int MAX_MEM = 10LL * (1 << 30);
	unsigned long long int M_BUFFER = 2LL * (1 << 20);
	int SZ_RATIO = 10;
	int NUM_LEVELS;
	int L1_BPE = bits_per_entry;

    NUM_LEVELS = (int) ceil(log((((double) MAX_MEM) / ((double) M_BUFFER)) * 
    			(((double) (SZ_RATIO-1)) / ((double) SZ_RATIO))) / log(SZ_RATIO));
    printf("NUM_LEVELS : %d\n", NUM_LEVELS);



	int filtered_levels =  (((double) L1_BPE) * pow(log(2),2)) / (log(SZ_RATIO));
    printf("filtered_levels : %d\n", filtered_levels);

    double p1 = exp(-L1_BPE * pow(log(2),2));
    printf("fpr at level 1 %f\n", p1);

	int w = (int) ceil(-log(pow(SZ_RATIO, 1) * p1) / pow(log(2),2));
	printf("level 2 bits_per_entry : %d\n", w);
	w = (int) ceil(-log(pow(SZ_RATIO, 2) * p1) / pow(log(2),2));
	printf("level 3 bits_per_entry : %d\n", w);



	int* arr = (int*) malloc(sizeof(int) * num_entries);
	for(int i = 0; i < num_entries; i++) {
		arr[i] = i;
	}


	int extra = ((num_entries * bits_per_entry) % 8 == 0 ? 0 : 1);

	char* bloom = (char*) malloc(((num_entries * bits_per_entry) / 8) + extra); 
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
	free(arr);
	free(bloom);
}


