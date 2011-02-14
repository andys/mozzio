#define _FILE_OFFSET_BITS 64
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
#include <stdint.h>
#include <pthread.h>

#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif


/* Mersenne Twister (public domain) */
#define MT_LEN 624
struct MT {
    int mt_index;
    unsigned long mt_buffer[MT_LEN];
};
void mt_init(struct MT *mt) {
	int i;
	for (i = 0; i < MT_LEN; i++)
		mt->mt_buffer[i] = rand();
	mt->mt_index = 0;
}
#define MT_IA           397
#define MT_IB           (MT_LEN - MT_IA)
#define UPPER_MASK      0x80000000
#define LOWER_MASK      0x7FFFFFFF
#define MATRIX_A        0x9908B0DF
#define TWIST(b,i,j)    ((b)[i] & UPPER_MASK) | ((b)[j] & LOWER_MASK)
#define MAGIC(s)        (((s)&1)*MATRIX_A)
unsigned long mt_random(struct MT *mt) { /* always returns numbers between 0 and 2^32-1 */
	unsigned long *b = mt->mt_buffer;
	int idx = mt->mt_index;
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
	mt->mt_index = idx + sizeof(unsigned long);
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
#define MAX_THREADS 1025
#define MOZZIO_RANDOM 0x01
#define MOZZIO_SEQUENTIAL 0x02
#define MOZZIO_READ 0x04
#define MOZZIO_WRITE 0x08
#define MOZZIO_FILE 0x10
#define MOZZIO_DEV 0x20

void fail(const char *errmsg)
{
	perror(errmsg);
	exit(1);
}

static struct MOZZIO_TEST_STATE {
        pthread_t thread;
        int thread_num;
        struct MT mt;
        int test_flags;
	double pre_start_time, run_time;
	volatile double start_time;
	intmax_t filesize, bytes_total;
	unsigned int block_size_kb;
	volatile intmax_t bytes_done, ios_done;
        volatile int finished_flag;
} state[MAX_THREADS];
#define global_state (state[MAX_THREADS-1])

static unsigned char random_data[RANDOM_DATA_BYTES];
static int fd;

void print_status_header(void);
void print_status(struct MOZZIO_TEST_STATE *state, const char *extra);


void *sync_thread(void *ptr)
{
    fsync(fd);
    *((int *)ptr) = 1;
    return(NULL);
}
void *test_thread(void *ptr)
{
    unsigned char *p, *buf;
    struct MOZZIO_TEST_STATE *mystate = (struct MOZZIO_TEST_STATE *)ptr;
    long done, todo = mystate->block_size_kb << 10;

    sleep(1);
    mystate->start_time = get_timestamp();
    
    if(mystate->test_flags & MOZZIO_WRITE && mystate->test_flags & MOZZIO_SEQUENTIAL)
    {
        while(mystate->bytes_done < mystate->bytes_total) 
        {
            if((done = write(fd, random_data, todo)) <= 0)
                fail("write error");

            mystate->bytes_done += done;
            mystate->ios_done++;
        }
    }
    else if(mystate->test_flags & MOZZIO_READ && mystate->test_flags & MOZZIO_SEQUENTIAL)
    {
       lseek(fd, 0, SEEK_SET);
       if(!(buf = malloc(todo)))
           fail("malloc");
       while(mystate->bytes_done < mystate->bytes_total) 
       {
           if((done = read(fd, buf, todo)) < todo)
               fail("read error");
           mystate->bytes_done += done;
           mystate->ios_done++;
       }
       free(buf);
    }
    else if(mystate->test_flags & MOZZIO_WRITE && mystate->test_flags & MOZZIO_RANDOM)
    {
        /*fprintf(stderr, "debug: %jd %jd %.1f %.1f\n", (intmax_t)mystate->bytes_total, (intmax_t)mystate->bytes_done, mystate->start_time, mystate->run_time);*/
        while((get_timestamp() - mystate->start_time) < mystate->run_time)
        {
            p = random_data + mt_random(&mystate->mt) % (RANDOM_DATA_BYTES - todo);
            if((done = pwrite(fd, p, todo, todo * (mt_random(&mystate->mt) % (mystate->filesize/todo)))) < todo)
                    fail("write error");
            fsync(fd);

            mystate->bytes_done += done;
            mystate->ios_done++;
        }
    }
    else if(mystate->test_flags & MOZZIO_READ && mystate->test_flags & MOZZIO_RANDOM)
    {
        if(!(buf = malloc(todo)))
            fail("malloc");
        while((get_timestamp() - mystate->start_time) < mystate->run_time)
        {
            p = random_data + mt_random(&mystate->mt) % (RANDOM_DATA_BYTES - todo);
            if((done = pread(fd, buf, todo, todo * (mt_random(&mystate->mt) % (mystate->filesize/todo)))) < todo)
                    fail("read error");
            mystate->bytes_done += done;
            mystate->ios_done++;
        }
        free(buf);
    }
    mystate->finished_flag = 1;
    return(NULL);
}

int collect_thread_stats(int num_threads)
{
    int thr, retval = 0;
    global_state.ios_done = 0;
    global_state.bytes_done = 0;
    for(thr=0; thr<num_threads; thr++)
    {
        global_state.bytes_done += state[thr].bytes_done;
        global_state.ios_done += state[thr].ios_done;
        if(!state[thr].finished_flag)
            retval++;
        if(!thr || global_state.start_time < state[thr].start_time)
            global_state.start_time = state[thr].start_time;
    }
    return(retval);
}

void perform_test(const char *path, int test_flags, int run_time, int block_size_kb, int file_size_gb, int write_size_mb, int num_threads)
{
    int thread, fd_flags = O_RDONLY, n = 1;
    pthread_t syncer;

    if(test_flags & MOZZIO_WRITE)
    {
        fd_flags = O_WRONLY | (test_flags & MOZZIO_RANDOM ? O_SYNC : O_TRUNC|O_APPEND);
    }
        
    if((fd = open(path, O_CREAT|O_LARGEFILE|fd_flags, 0666))<0)
        fail(path);

    global_state.run_time = (double) run_time;
    global_state.filesize = (intmax_t) file_size_gb << 30;
    global_state.bytes_total = (intmax_t) write_size_mb << 20;
    global_state.start_time = 0;
    global_state.pre_start_time = get_timestamp();
    global_state.block_size_kb = block_size_kb;
    global_state.test_flags = test_flags;
    global_state.thread_num = num_threads;
    
    for(thread=0; thread<num_threads; thread++)
    {
        mt_init(&state[thread].mt);
        state[thread].thread_num = thread;
        state[thread].pre_start_time = global_state.pre_start_time;
        state[thread].run_time = global_state.run_time;
        state[thread].filesize = global_state.filesize;
        state[thread].bytes_total = (global_state.bytes_total ? global_state.bytes_total : global_state.filesize) / num_threads;
        state[thread].ios_done = 0;
        state[thread].bytes_done = 0;
        state[thread].block_size_kb = block_size_kb;
        state[thread].test_flags = test_flags;
        state[thread].finished_flag = 0;
        pthread_create((pthread_t *) &(state[thread].thread), NULL, test_thread, (void *) &state[thread]);
    }

    while(n > 0)
    {
        sleep(1);
        n = collect_thread_stats(num_threads);
        print_status(&global_state, "");
    }
    if(test_flags & MOZZIO_WRITE) 
    {
        print_status(&global_state, "SYNC");
        n = 0;
        pthread_create(&syncer, NULL, sync_thread, (void *)&n);
        while(!n)
        {
            sleep(1);
            print_status(&global_state, "SYNC");
        }
        pthread_join(syncer, NULL);
    }
    for(thread=0; thread<num_threads; thread++)
    {
        pthread_join(state[thread].thread, NULL);
    }
    print_status(&global_state, "OK  \n");
    close(fd);
}


void print_help(const char *extra)
{
    fprintf(stderr, "mozzio - Disk benchmark - http://mozzio.org\n\
Options:\n\
  -h           This help\n\
  -p path      Path to benchmark file (default: mozzio.bin)n\n\
  -d device    Path to benchmark device (use only -p or -d)\n\
  -b blcksize  Block size in kB  (default: 4kB random, 1024kB sequential)\n\
  -s filesize  File/device size in GB  (default: 10GB) (for seq write test)\n\
  -r runtime   Test run time in seconds (default: 30s (for all but seq write)\n\
  -w writesize For devices, amount of data to seq write\n\
  -t threads   Number of threads to use (default: 2 for seq, 256 for rand)\n\
    %s\n\
", extra);
    exit(1);
}

void init_random_data(void)
{
    unsigned long int i;
    /* initialise random data */
    mt_init(&global_state.mt);
    for(i=0; i<RANDOM_DATA_BYTES>>2; i+=4) {
        *((unsigned long *) (random_data + i)) = mt_random(&global_state.mt);
    }
}

int main(int argc, char *argv[])
{
	unsigned long int blocksize = 4096;
	off_t filesize = 5 * 1024 * 1024 * 1024LL;
	unsigned long int num_writes = 3000;
	unsigned char *p;
	unsigned long int i, x;
	double start_time;
	
	char *path = "mozzio.bin";
	int optchar, test_options=0, num_threads=256, run_time=30, file_size=10, block_size=4, write_size=0;

	while((optchar=getopt(argc, argv, "p:d:b:s:r:w:t:h?")) != -1)
	{
	    switch(optchar)
	    {
	        case 'b':
	            block_size = atoi(optarg);
	            if(block_size <= 0 || block_size > 1024)
	                print_help("invalid block size in kB");
                    break;
                case 's':
                    file_size = atoi(optarg);
                    if(file_size <= 0)
                        print_help("invalid file size in GB");
                case 'r':
                    /*run_time = atoi(optarg);*/
                    if(run_time <= 0)
                        print_help("invalid run time in seconds");
                    break;
                case 'w':
                    write_size = atoi(optarg);
                    if(write_size <= 0)
                        print_help("invalid write size in MB");
                    break;
	        case 'p':
	            path = (char *) strdup(optarg);
	            if(test_options & MOZZIO_DEV)
	                print_help("cannot use -d and -p at the same time");
	            test_options |= MOZZIO_FILE;
	            break;
                case 'd':
                    path = (char *) strdup(optarg);
	            test_options |= MOZZIO_DEV;
	            if(test_options & MOZZIO_FILE)
	                print_help("cannot use -d and -p at the same time");
	            break;
                case 't':
                    num_threads = atoi(optarg);
                    if(num_threads < 1 || num_threads > (MAX_THREADS-1))
                        print_help("invalid number of threads");
                    break;
	        default:
	        case 'h':
	        case '?':
                    print_help("");
	    }
	}
	if(write_size > 0 && !(test_options & MOZZIO_DEV))
	    print_help("write size can only be set when using a device");
        write_size = file_size << 10;
        if(!(test_options & (MOZZIO_FILE | MOZZIO_DEV)))
            test_options |= MOZZIO_FILE;

        init_random_data();
	
	fprintf(stderr, "Starting mozzio. threads: %d, run time: %d, file size: %d, block size: %d, write size: %d\n", num_threads, run_time, file_size, block_size, write_size);
	
	print_status_header();

        perform_test(path, test_options | MOZZIO_WRITE | MOZZIO_SEQUENTIAL, run_time, RANDOM_DATA_BYTES>>10, file_size, write_size, 1);
        perform_test(path, test_options | MOZZIO_READ  | MOZZIO_SEQUENTIAL, run_time, RANDOM_DATA_BYTES>>10, file_size, write_size, 1);
        perform_test(path, test_options | MOZZIO_WRITE | MOZZIO_RANDOM,     run_time, 4, file_size, 0, 256);
        perform_test(path, test_options | MOZZIO_READ  | MOZZIO_RANDOM,     run_time, 4, file_size, 0, 256);

	return(0);
}


void print_status_header(void)
{
	fprintf(stderr, "             Thrds Block/kB File/GB Time/sec Done/MB IOPS   Byte rate  Progress\n");
}

void print_status(struct MOZZIO_TEST_STATE *state, const char *extra)
{
	double current_time = get_timestamp();
	double secs_done=0;
	float p2, progress=0;

	if(state->start_time && state->start_time < current_time) {
	    if(state->run_time)
                progress = (float) ((current_time - state->start_time) / state->run_time);
            secs_done = current_time - state->start_time;
        }
	if(state->bytes_total) {
		progress = (float) state->bytes_done / (float) state->bytes_total;
	}
	if(progress>1)
	    progress = 1;
	
	fprintf(stderr, "\r%-4s %-7s %-5u %-8u %-7jd %-8d %-7jd %-6jd %-6jdMB/s %5.1f%% %s",
		state->test_flags & MOZZIO_RANDOM ? "Rand" : "Seq",
		state->test_flags & MOZZIO_WRITE ? "Write" : "Read",
		state->thread_num,
		state->block_size_kb,
		state->filesize>>30,
		(int) secs_done,
		state->bytes_done>>20,
		secs_done>0 ? (intmax_t) ((double)state->ios_done / secs_done) : 0,
		secs_done>0 ? (intmax_t) ((double)(state->bytes_done / secs_done))>>20 : 0,
		progress * 100,
		extra
	);
}

/*
0123456789  0123456789012 012345678901 012345678 01234567 01234     0123
            Block(kB) Size(GB) Time(sec) Done(MB) Xfer Rate            Progress
Seq  Read   128       4        900       32768    12345io/s 1234MB/s   99.9%    
Seq  Write  4         
Rand Read   16  
Rand Write  16-64     

1. progress went to 100% too early


*/






