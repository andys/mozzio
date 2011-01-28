#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif


/* Mersenne Twister (public domain) */
#define MT_LEN 624
int mt_index;
unsigned long mt_buffer[MT_LEN];
void mt_init() {
	int i;
	for (i = 0; i < MT_LEN; i++)
		mt_buffer[i] = rand();
	mt_index = 0;
}
#define MT_IA           397
#define MT_IB           (MT_LEN - MT_IA)
#define UPPER_MASK      0x80000000
#define LOWER_MASK      0x7FFFFFFF
#define MATRIX_A        0x9908B0DF
#define TWIST(b,i,j)    ((b)[i] & UPPER_MASK) | ((b)[j] & LOWER_MASK)
#define MAGIC(s)        (((s)&1)*MATRIX_A)
unsigned long mt_random() { /* always returns numbers between 0 and 2^32-1 */
	unsigned long * b = mt_buffer;
	int idx = mt_index;
	unsigned long s;
	int i;

	if (idx == MT_LEN*sizeof(unsigned long))
	{
		idx = 0;
		i = 0;
		for (; i < MT_IB; i++) {
			s = TWIST(b, i, i+1);
			b[i] = b[i + MT_IA] ^ (s >> 1) ^ MAGIC(s);
		}
		for (; i < MT_LEN-1; i++) {
			s = TWIST(b, i, i+1);
			b[i] = b[i - MT_IB] ^ (s >> 1) ^ MAGIC(s);
		}
		
		s = TWIST(b, MT_LEN-1, 0);
		b[MT_LEN-1] = b[MT_IA-1] ^ (s >> 1) ^ MAGIC(s);
	}
	mt_index = idx + sizeof(unsigned long);
	return *(unsigned long *)((unsigned char *)b + idx);
}
/* end of Mersenne Twister */



double get_timestamp(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return((double) tv.tv_sec + ((double) tv.tv_usec / 1000000));
}

#define RANDOM_DATA_BYTES 1048576

void fail(const char *errmsg)
{
	perror(errmsg);
	exit(1);
}

int main(int argc, char *argv[])
{
	unsigned long int blocksize = 4096;
	off_t filesize = 5 * 1024 * 1024 * 1024LL;
	unsigned long int num_writes = 3000;
	unsigned char random_data[RANDOM_DATA_BYTES], *p;
	unsigned long int i, x;
	double start_time;
	int fd;

	mt_init(); /* initialise random number generator */

	printf("Generating random data... ");
	for(i=0; i<RANDOM_DATA_BYTES>>2; i+=4) {
		*((unsigned long *) (random_data + i)) = mt_random();
	}
	printf("done.\n");

	if((fd = open("mozzio.bin", O_CREAT|O_TRUNC|O_LARGEFILE|O_RDWR, 0666))<0)
		fail("mozzio.bin");

	start_time = get_timestamp();
	for(i=0; i<(filesize / RANDOM_DATA_BYTES); i++) {
		//*((unsigned long int *) random_data) = i; 
		if(write(fd, random_data, RANDOM_DATA_BYTES) < RANDOM_DATA_BYTES)
			fail("write error");
	}
	fsync(fd);
	start_time = get_timestamp() - start_time;
	printf("Sequential Write: %luMB @ %.1fMB/s\n", filesize>>20, (double)filesize/start_time/1048576);
	
	lseek(fd, 0, SEEK_SET);
	
	start_time = get_timestamp();
	for(i=0; i<(filesize / RANDOM_DATA_BYTES); i++) {
		if(read(fd, random_data, RANDOM_DATA_BYTES) < RANDOM_DATA_BYTES)
			fail("read error");
	}
	start_time = get_timestamp() - start_time;
	printf("Sequential Read: %luMB @ %.1fMB/s\n", filesize>>20, (double)filesize/start_time/1048576);
	
	start_time = get_timestamp();
	for(i=0; i<num_writes; i++) {
		x = mt_random();
		p = random_data + (blocksize * (x % RANDOM_DATA_BYTES/blocksize));
		//*((unsigned long int *) p) = i;
		if(pwrite(fd, p, blocksize, blocksize * (x % (filesize/blocksize))) < blocksize)
			fail("write error");
		fsync(fd);
	}
	start_time = get_timestamp() - start_time;
	printf("4kB Random Write: %luMB @ %.1fMB/s\n", (num_writes * blocksize)>>20, (double)(num_writes * blocksize)/start_time/1048576);
	
	num_writes *= 10;

	start_time = get_timestamp();
	for(i=0; i<num_writes; i++) {
		x = mt_random();
		if(pread(fd, random_data, blocksize, blocksize * (x % (filesize/blocksize))) < blocksize)
			fail("read error");
	}
	start_time = get_timestamp() - start_time;
	printf("4kB Random Read: %luMB @ %.1fMB/s\n", (num_writes * blocksize)>>20, (double)(num_writes * blocksize)/start_time/1048576);
	
	return(0);
}

