#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <unistd.h>

// Initialize semaphores and counters
sem_t resource;
pthread_mutex_t rmutex;
int readCount = 0;

// Function definitions
void read_acquire() {
    pthread_mutex_lock(&rmutex);
    readCount++;
    if (readCount == 1) {
        sem_wait(&resource);
    }
    pthread_mutex_unlock(&rmutex);
}

void read_release() {
    pthread_mutex_lock(&rmutex);
    readCount--;
    if (readCount == 0) {
        sem_post(&resource);
    }
    pthread_mutex_unlock(&rmutex);
}

void write_acquire() {
    sem_wait(&resource);
}

void write_release() {
    sem_post(&resource);
}


void* reader(void* arg) {
    long id = (long)arg;
    while (1) {
        read_acquire();
        printf("Reader %ld is reading\n", id);
        sleep(1);  // Simulate reading
        read_release();
        sleep(1);  // Simulate time between reads
    }
    return NULL;
}

void* writer(void* arg) {
    long id = (long)arg;
    while (1) {
        write_acquire();
        printf("Writer %ld is writing\n", id);
        sleep(1);  // Simulate writing
        write_release();
        sleep(1);  // Simulate time between writes
    }
    return NULL;
}

int main() {
    pthread_t r[5], w[2];

    // Initialize semaphores and mutexes
    sem_init(&resource, 0, 1);
    pthread_mutex_init(&rmutex, NULL);

    // Create reader and writer threads
    for (long i = 0; i < 5; i++) {
        pthread_create(&r[i], NULL, reader, (void*)i);
    }
    for (long i = 0; i < 2; i++) {
        pthread_create(&w[i], NULL, writer, (void*)i);
    }

    // Join threads (this example will run indefinitely)
    for (int i = 0; i < 5; i++) {
        pthread_join(r[i], NULL);
    }
    for (int i = 0; i < 2; i++) {
        pthread_join(w[i], NULL);
    }

    // Destroy semaphores and mutexes
    sem_destroy(&resource);
    pthread_mutex_destroy(&rmutex);

    return 0;
}


