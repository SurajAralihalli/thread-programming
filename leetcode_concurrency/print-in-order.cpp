// Author: Suraj Aralihalli
// Url: https://leetcode.com/problems/print-in-order/description/
// Date: 18th Feb, 2023
// Tags: concurrency, thread, semaphore 

// Approach 1
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>

class Foo {
public:
    sem_t f;
    sem_t s;
    sem_t t;
    Foo() {
        sem_init(&f, 0, 1);
        sem_init(&s, 0, 0);
        sem_init(&t, 0, 0);
    }

    void first(function<void()> printFirst) {
        
        // printFirst() outputs "first". Do not change or remove this line.
        sem_wait(&f);
        printFirst();
        sem_post(&s);
    }

    void second(function<void()> printSecond) {
        
        // printSecond() outputs "second". Do not change or remove this line.
        sem_wait(&s);
        printSecond();
        sem_post(&t);
    }

    void third(function<void()> printThird) {
        
        // printThird() outputs "third". Do not change or remove this line.
        sem_wait(&t);
        printThird();
        sem_post(&f);
    }
};


// Approach 2
class Foo {
public:
    pthread_mutex_t lock;
    pthread_cond_t cv;
    int i;
    Foo() {
        pthread_mutex_init(&lock, NULL);
        pthread_cond_init(&cv, NULL);
        i = 1;
    }

    void first(function<void()> printFirst) {
        
        pthread_mutex_lock(&lock);
        while(!(i==1)) {
            pthread_cond_wait(&cv, &lock);
        }
        // printFirst() outputs "first". Do not change or remove this line.
        printFirst();
        i=2;
        pthread_cond_broadcast(&cv);
        pthread_mutex_unlock(&lock);
    }

    void second(function<void()> printSecond) {
        
        pthread_mutex_lock(&lock);
        while(!(i==2)) {
            pthread_cond_wait(&cv, &lock);
        }
        // printSecond() outputs "second". Do not change or remove this line.
        printSecond();
        i=3;
        pthread_cond_broadcast(&cv);
        pthread_mutex_unlock(&lock);
    }

    void third(function<void()> printThird) {
        
        pthread_mutex_lock(&lock);
        while(!(i==3)) {
            pthread_cond_wait(&cv, &lock);
        }
        // printThird() outputs "third". Do not change or remove this line.
        printThird();
        i=1;
        pthread_cond_broadcast(&cv);
        pthread_mutex_unlock(&lock);
    }
};
