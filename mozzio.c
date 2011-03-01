#define MOZZIO_VER "0.3"
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
    uint32_t mt_buffer[MT_LEN];
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
uint32_t mt_random(struct MT *mt) { /* always returns numbers between 0 and 2^32-1 */
	uint32_t *b = mt->mt_buffer;
	int idx = mt->mt_index;
	uint32_t s;
	int i;

	if (idx == MT_LEN*sizeof(uint32_t))
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
	mt->mt_index = idx + sizeof(uint32_t);
	return *(uint32_t *)((unsigned char *)b + idx);
}
/* end of Mersenne Twister */



double get_timestamp(void)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return((double) tv.tv_sec + ((double) tv.tv_usec / 1000000));
}

#define RANDOM_DATA_BYTES 1048576
#define MAX_THREADS 256
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
	intmax_t filesize, bytes_total;
	unsigned int block_size_kb;
	volatile intmax_t bytes_done, ios_done;
        volatile double start_time, end_time;
        volatile int finished_flag;
} state[MAX_THREADS+1];
#define global_state (state[MAX_THREADS])

#define MOZZIO_STOPPED 0
#define MOZZIO_STARTING 1
#define MOZZIO_RUNNING 2
static volatile int state_flag;
static unsigned char random_data[RANDOM_DATA_BYTES];
static int fd;
static double run_time=30;

void print_status_header(void);
void print_status(struct MOZZIO_TEST_STATE *state, const char *extra, double secs_done);

void *sync_thread(void *ptr)
{
    fsync(fd);
    *(double *)ptr = get_timestamp();
    return(NULL);
}

void *test_thread(void *ptr)
{
    unsigned char *p, *buf;
    struct MOZZIO_TEST_STATE *mystate = (struct MOZZIO_TEST_STATE *)ptr;
    long done, todo = mystate->block_size_kb << 10;
    long *lp;
    off_t my_offset;

/*    if(mystate->thread_num==0)
        lseek(fd, 0, SEEK_SET);*/

    sleep(1);
    
    p = random_data + mystate->thread_num * (RANDOM_DATA_BYTES / MAX_THREADS);
    lp = (long *) p;
    
    if(!(buf = malloc(todo)))
        fail("malloc");

    mystate->start_time = get_timestamp();

    while(state_flag && (!mystate->bytes_total || mystate->bytes_done < mystate->bytes_total))
    {
        if(mystate->test_flags & MOZZIO_SEQUENTIAL)
        {
            if(mystate->test_flags & MOZZIO_WRITE) 
                done = write(fd, random_data, todo);
            else
                done = read(fd, buf, todo);
        }
        else
        {
            my_offset = (mt_random(&mystate->mt) % mystate->filesize) & ~(todo-1);
            if(mystate->test_flags & MOZZIO_WRITE)
                done = pwrite(fd, p, todo, my_offset);
            else
                done = pread(fd, buf, todo, my_offset);
        }
        if(done != todo) 
            fail("IO error");
        if(mystate->bytes_total || state_flag != MOZZIO_STARTING)
        {
            mystate->bytes_done += done;
            mystate->ios_done++;
        }
        (*lp)++;
    }
    if(mystate->bytes_total)
        fsync(fd);
    mystate->end_time = get_timestamp();
    mystate->finished_flag = 1;
    free(buf);
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

void drop_linux_vm_cache(void) {
    FILE *fp = fopen("/proc/sys/vm/drop_caches", "w");
    if(fp)
    {
        fputs("1\n", fp);
        fclose(fp);
    }
}

void perform_test(const char *path, int test_flags, int block_size_kb, int file_size_gb, int num_threads)
{
    int thread, fd_flags = O_RDONLY, n = 1;
    double start_time, current_time;
    volatile double end_time = 0;

    if(test_flags & MOZZIO_WRITE)
        fd_flags = O_WRONLY | (test_flags & MOZZIO_RANDOM ? O_SYNC : O_TRUNC|O_APPEND);
    else
        drop_linux_vm_cache();
        
    if((fd = open(path, O_CREAT|O_LARGEFILE|fd_flags, 0666))<0)
        fail(path);

    global_state.filesize = (intmax_t) file_size_gb << 30;
    global_state.block_size_kb = block_size_kb;
    global_state.test_flags = test_flags;
    global_state.thread_num = num_threads;
    global_state.bytes_total = test_flags & MOZZIO_SEQUENTIAL ? global_state.filesize : 0;
    state_flag = num_threads>1 ? MOZZIO_STARTING : MOZZIO_RUNNING;

    for(thread=0; thread<num_threads; thread++)
    {
        mt_init(&state[thread].mt);
        state[thread].thread_num = thread;
        state[thread].filesize = global_state.filesize;
        state[thread].bytes_total = global_state.bytes_total / num_threads;
        state[thread].ios_done = 0;
        state[thread].bytes_done = 0;
        state[thread].block_size_kb = block_size_kb;
        state[thread].test_flags = test_flags;
        state[thread].finished_flag = 0;
        pthread_create((pthread_t *) &(state[thread].thread), NULL, test_thread, (void *) &state[thread]);
    }

    if(state_flag == MOZZIO_STARTING)
    {
        for(thread=-3; thread<0; thread++)
        {
            print_status(&global_state, "", thread);
            sleep(1);
        }
        state_flag = MOZZIO_RUNNING;
    }

    start_time = current_time = get_timestamp();
    
    while(n > 0)
    {
       if(!global_state.bytes_total && (current_time - start_time) >= run_time)
       {
           end_time = current_time;
           state_flag = MOZZIO_STOPPED;
        }
        sleep(1);
        
        n = collect_thread_stats(num_threads);
        print_status(&global_state, "", current_time - start_time);
        current_time = get_timestamp();
    }
    if(end_time == 0) {
        end_time = state[0].end_time;
        start_time = state[0].start_time;
    }

    for(thread=0; thread<num_threads; thread++)
        pthread_join(state[thread].thread, NULL);

    print_status(&global_state, "OK  \n", end_time - start_time);
    close(fd);
}


void print_help(const char *extra)
{
    fprintf(stderr, "mozzio - Disk benchmark - http://mozzio.org\n\
Options:\n\
  -h           This help\n\
  -p path      Path to benchmark file or device (default: mozzio.bin)\n\
  -b blcksize  Block size in kB  (default: 4kB random, 128kB sequential)\n\
  -s filesize  File/device size in GB  (default: 10GB) (for seq write test)\n\
  -r runtime   Test run time in seconds (default: 30s (for all but seq write)\n\
  -t threads   Number of threads to use (default: 2 for seq, 128 for rand)\n\
    %s\n\
", extra);
    exit(1);
}

void init_random_data(void)
{
    uint32_t i;
    /* initialise random data */
    mt_init(&global_state.mt);
    for(i=0; i<RANDOM_DATA_BYTES>>2; i+=4) {
        *((uint32_t *) (random_data + i)) = mt_random(&global_state.mt);
    }
}

int main(int argc, char *argv[])
{
	char *path = "mozzio.bin";
	int optchar, test_options=0, num_threads=128, file_size=10, block_size=4;

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
                    break;
                case 'r':
                    run_time = (double) atoi(optarg);
                    if(run_time <= 0)
                        print_help("invalid run time in seconds");
                    break;
	        case 'p':
	            path = (char *) strdup(optarg);
	            if(test_options & MOZZIO_DEV)
	                print_help("cannot use -d and -p at the same time");
	            test_options |= MOZZIO_FILE;
	            break;
                case 't':
                    num_threads = atoi(optarg);
                    if(num_threads < 1 || num_threads > MAX_THREADS)
                        print_help("invalid number of threads");
                    break;
	        default:
	        case 'h':
	        case '?':
                    print_help("");
	    }
	}

        init_random_data();
	
	fprintf(stderr, "Starting mozzio v%s.\n(Numthreads: %d, Runtime: %ds, Filesize: %dG, Blocksize: 128K,%dK)\n", MOZZIO_VER, num_threads, (int)run_time, file_size, block_size);
	
	print_status_header();

        perform_test(path, test_options | MOZZIO_WRITE | MOZZIO_SEQUENTIAL, 128,        file_size, 1);
        perform_test(path, test_options | MOZZIO_READ  | MOZZIO_SEQUENTIAL, 128,        file_size, 1);
        perform_test(path, test_options | MOZZIO_WRITE | MOZZIO_RANDOM,     block_size, file_size, num_threads);
        perform_test(path, test_options | MOZZIO_READ  | MOZZIO_RANDOM,     block_size, file_size, num_threads);

	return(0);
}


void print_status_header(void)
{
	fprintf(stderr, "              Thrds Block/kB File/GB Time/sec Done/MB IOPS   Byte rate  Progress\n");
}

void print_status(struct MOZZIO_TEST_STATE *state, const char *extra, double secs_done)
{
	double progress;
	if(state->bytes_total) {
	    progress = (double) state->bytes_done / (double) state->bytes_total;
	}
	else {
	    progress = secs_done / run_time;
	}
	if(progress>1)
	    progress = 1;
        else if(progress<0)
            progress = 0;
	
	fprintf(stderr, "\r%-5s %-7s %-5u %-8u %-7jd %-8d %-7jd %-6jd %-6jdMB/s %5.1f%% %s",
		state->test_flags & MOZZIO_RANDOM ? "Randm" : "Seqn.",
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
