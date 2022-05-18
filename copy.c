/* 
Family Name: Thakkar 
Given Name: Arpit Nileshbhai
Student Number: 217632340
CS Login: arpit21
YorkU email address (the one that appears in eClass): arpit21@my.yorku.ca
*/ 

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <semaphore.h>
#include <pthread.h>
#include <math.h>
#include <unistd.h>
#include <math.h>
/* For opening the file with O_RDONLY */
#include <sys/stat.h>
#include <fcntl.h>

/* Defining ten milliseconds in nanos */
#define TEN_MILLIS_IN_NANOS 1000000

/* Struct as per given in file */

typedef struct{
    char data;
    off_t offset;
} BufferItem;

/* Global Variables */

int consumer_threads;
int producer_threads;
int buffer_size;
int i = 0;
int fin;
int fout;
int fout_log;
BufferItem *buffer;
int insertPointer = 0;
int removePointer = 0;
int consumersWaitingCounter = 0;
int leftProducers = 0;
int loglinecount;
sem_t empty;
sem_t full;

/* Creating the mutexes for locking and unlocking itt whenever needed */

pthread_mutex_t mainBufferLock;
pthread_mutex_t readLock;
pthread_mutex_t writeLock;
pthread_mutex_t logLock;
pthread_mutex_t consumerswaitingLock;

/* Temporary buffer array */

char temp_buffer[80];

/* Helper Functions */


/* Nanosleep function */

int nanoSleep(){
    struct timespec time = {0,0};
    time.tv_sec = 0;
    time.tv_nsec = rand()%TEN_MILLIS_IN_NANOS;
    if(nanosleep(&time, NULL) < 0){
        fprintf(stderr,"Nano sleep system call failed\n");
        exit(1);
    }

    return 0;
}

/* toLog function - which will output everything 
    it will print in the terminal as well as 
    it will log everything in the log file */

void toLog(char * name, int thr, BufferItem *i, int num, char* acro){
    pthread_mutex_lock(&logLock);
    loglinecount++;

    fprintf(stderr,"%d: %s %s%d O%ld B%d I%d\n",loglinecount ,name, acro, thr, i->offset, i->data, num);

    size_t line_length = snprintf(temp_buffer,80,"%s %s%d O%ld B%d I%d\n", name, acro, thr, i->offset, i->data, num);
    write(fout_log,&temp_buffer,line_length);
    pthread_mutex_unlock(&logLock);


}

/* This function will read from the input file fin and will log the read_byte in the log file */

void readByte(int thread, BufferItem *item){
    pthread_mutex_lock(&readLock);

    if((item->offset = lseek(fin, 0, SEEK_CUR)) < 0){
        pthread_mutex_unlock(&readLock);
        fprintf(stderr,"Cannot seek output file.\n");
        exit(1);
    }

    if(read(fin, &(item->data),1) < 1){
        leftProducers++;
        pthread_mutex_unlock(&readLock);
        pthread_exit(0);
    }

    toLog("read_byte", thread, item, -1, "PT");
    pthread_mutex_unlock(&readLock);
}

/* This write funtion will write to the log file */

void writeByte(int thread, BufferItem *item){
    pthread_mutex_lock(&writeLock);

    if(lseek(fout, item->offset, SEEK_SET) < 0){
        pthread_mutex_unlock(&writeLock);
        fprintf(stderr,"Cannot seek output file.\n");
        exit(1);
    }

    if(write(fout, &(item->data), 1) < 1){
        pthread_mutex_unlock(&writeLock);
        fprintf(stderr,"Cannot write to output file\n");
        exit(1);
    }

    toLog("write_byte", thread, item, -1, "CT");
    pthread_mutex_unlock(&writeLock);
}

/* Produce function first call nanosleep() which will generate
    random number of time for which the function will sleep and then 
    it will call readByte to pass the variables and then again nanosleep()
    and then we log the activity of produce funtion to the log file using toLog() function. */

void *produce(void *param){
        int thread = *((int *)param);
    while(1){
        nanoSleep();
        BufferItem item;
        readByte(thread, &item);
        nanoSleep();
        sem_wait(&empty);
        pthread_mutex_lock(&mainBufferLock);
        buffer[insertPointer] = item;
        toLog("produce",thread, &item, insertPointer, "PT");
        insertPointer = (insertPointer+1) % buffer_size;
        pthread_mutex_unlock(&mainBufferLock);
        sem_post(&full);
    }
}

/* Consume function will first let the produce finish the work 
    and meanwhile it will call the nanosleep() function for some random number
    of time. Then I have used locks to lock the mutex and then increment the counter 
    and then unlocks the mutex and them process repeats again and decrements the counter.
    Then the consume function will log the activity in the log file. */

void *consume(void *param){
        int thread = *((int *)param);
    while(1){
        nanoSleep();
        pthread_mutex_lock(&consumerswaitingLock);
        consumersWaitingCounter++;
        pthread_mutex_unlock(&consumerswaitingLock);
        sem_wait(&full);
        pthread_mutex_lock(&consumerswaitingLock);
        consumersWaitingCounter--;
        pthread_mutex_unlock(&consumerswaitingLock);

        pthread_mutex_lock(&mainBufferLock);
        BufferItem item = buffer[removePointer]; 
        toLog("consume",thread, &item, removePointer, "CT");
        removePointer = (removePointer+1) % buffer_size;
        pthread_mutex_unlock(&mainBufferLock);
        sem_post(&empty);
        nanoSleep();
        writeByte(thread, &item);
    }
}

/* This function will help to exit the main function. 
    We will check all the pointers which we entered as IN thread and OUT thread
    and if that becomes equal that means we got what we wanted so  the loop will exit. */

void *WaitToFinish(void *param){
        while(1){
           sleep(1);

        if(removePointer == insertPointer  && consumersWaitingCounter == consumer_threads && leftProducers == producer_threads)  {
            fprintf(stderr,"\n\tconsumers done\n");
            pthread_exit(0);
        }
    }
}

/* Main execution loop */

int main(int argc, char *argv[]){

    /* Error handling */

    if (argc-1 != 6){
        printf("\nWRONG ARGUMENTS\n");
        exit(0);
    }

    /* Assigning the variables according to the arguements we enter */

    producer_threads = atoi(argv[1]);
    buffer_size = atoi(argv[5]);
    consumer_threads = atoi(argv[2]);

    /* fin is the input file. 
    I used remove() to remove the previously created file 
    and then fout is the file that we copied for dataset
    provided to us. If the fout is not there then it will be created
    using the O_CREAT permission. Same for fout_log. */

    fin = open(argv[3], O_RDONLY);
    remove(argv[4]);
    remove(argv[6]);
    fout = open(argv[4], O_RDWR | O_CREAT, 0644);
    fout_log = open(argv[6], O_RDWR | O_CREAT, 0644);

    /* Initializing the mutexes with NULL. */

    pthread_mutex_init(&mainBufferLock,NULL);
    pthread_mutex_init(&readLock, NULL);
    pthread_mutex_init(&writeLock,NULL);
    pthread_mutex_init(&logLock, NULL);
    pthread_mutex_init(&consumerswaitingLock,NULL);

    /* Initializing semaphores */

    sem_init(&empty, 0, buffer_size);
    sem_init(&full, 0, 0);

    /* Creating IN and OUT thread and allocating memory to them */

    pthread_t *in_thread;
    pthread_t *out_thread;

    in_thread = malloc(producer_threads * sizeof(pthread_t));
    out_thread = malloc(producer_threads * sizeof(pthread_t));
    buffer = (BufferItem*) malloc(buffer_size * sizeof(BufferItem));

    /* Creating the produce thread */
    
    for(i=0; i<producer_threads; i++){
        int *sendI = malloc(sizeof(*sendI));
        *sendI = i;
        pthread_create(&in_thread[i], NULL, produce, sendI);
    }

    /* Creating the consumer thread */

    for(i=0; i<consumer_threads; i++){
        int *sendI = malloc(sizeof(*sendI));
        *sendI = i;
        pthread_create(&out_thread[i], NULL, consume, sendI);
    }

    /* Joining the producer thread */

    for(i=0; i<producer_threads; i++){
        pthread_join(in_thread[i], NULL);
    }

    /* Calling the function that will help exit the main loop */

    pthread_t wait_to_finish;
    pthread_create(&wait_to_finish,NULL,WaitToFinish,NULL);
    pthread_join(wait_to_finish,NULL);

    /* Destroying all the mutexes */

    pthread_mutex_destroy(&mainBufferLock);
    pthread_mutex_destroy(&readLock);
    pthread_mutex_destroy(&writeLock);
    pthread_mutex_destroy(&logLock);
    pthread_mutex_destroy(&consumerswaitingLock);

    /* Closing all the files */

    close(fin);
    close(fout);
    close(fout_log);

    return 0;
}