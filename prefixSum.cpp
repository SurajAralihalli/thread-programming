#include <pthread.h>
#include <bits/stdc++.h>

using namespace std;

#define NUM_LEAF_THREADS 32

struct prefixArgs {
    int* arr;
    int* prefixSumArr;
    int* treeArr;
    pthread_cond_t* cvArr;
    pthread_mutex_t* mutexArr;
    int n;
    int id;

    prefixArgs(int* a, int* p, int* t, pthread_cond_t* c, pthread_mutex_t* m,int n1, int i) : arr(a), prefixSumArr(p), treeArr(t), cvArr(c), mutexArr(m), n(n1), id(i) {}
};

void* prefixWorkerBody(void* arg) {
    prefixArgs* pArg = (prefixArgs*)arg;

    int* arr = pArg->arr;
    int* treeArr = pArg->treeArr;
    pthread_cond_t* cvArr = pArg->cvArr;
    pthread_mutex_t* mutexArr = pArg->mutexArr;
    int id = pArg->id;

    int sum = 0;

    int childId1 = 2*id + 1;
    int childId2 = 2*id + 2;

    pthread_mutex_lock(&mutexArr[childId1]);
    while(treeArr[childId1] == -1) {
        pthread_cond_wait(&cvArr[childId1], &mutexArr[childId1]);
    }
    sum += treeArr[childId1];
    pthread_mutex_unlock(&mutexArr[childId1]);


    pthread_mutex_lock(&mutexArr[childId2]);
    while(treeArr[childId2] == -1) {
        pthread_cond_wait(&cvArr[childId2], &mutexArr[childId2]);
    }
    sum += treeArr[childId2];
    pthread_mutex_unlock(&mutexArr[childId2]);


    pthread_mutex_lock(&mutexArr[id]);
    treeArr[id] = sum;
    pthread_cond_broadcast(&cvArr[id]);
    pthread_mutex_unlock(&mutexArr[id]);

    return NULL;
    
}

void* prefixWorkerLeaf(void* arg) {
    prefixArgs* pArg = (prefixArgs*)arg;

    int* arr = pArg->arr;
    int* treeArr = pArg->treeArr;
    int* prefixSumArr = pArg->prefixSumArr;
    pthread_cond_t* cvArr = pArg->cvArr;
    pthread_mutex_t* mutexArr = pArg->mutexArr;
    int id = pArg->id;
    int n = pArg->n;


    int leaf_id = id + 1 - NUM_LEAF_THREADS;
    int block_size = n / NUM_LEAF_THREADS;
    int n_ = block_size;

    if(leaf_id == (NUM_LEAF_THREADS-1)) {
        n_ += (n % NUM_LEAF_THREADS);
    }

    int sum = 0;

    for(int i = (leaf_id*block_size); i < (leaf_id*block_size) + n_ ; i++) {
        sum += arr[i];
    }

    pthread_mutex_lock(&mutexArr[id]);
    treeArr[id] = sum;
    pthread_cond_broadcast(&cvArr[id]);
    pthread_mutex_unlock(&mutexArr[id]);

    sum = 0;
    //even number
    if(id%2==0) { 
        int siblingId = id-1;
        pthread_mutex_lock(&mutexArr[siblingId]);
        while(treeArr[siblingId] == -1) {
            pthread_cond_wait(&cvArr[siblingId], &mutexArr[siblingId]);
        }
        sum += treeArr[siblingId];
        pthread_mutex_unlock(&mutexArr[siblingId]);
    }

    int curId = id;
    while(true) {
        int parent = ceil( (float)curId/ 2 ) - 1;
        int aunt = parent - 1;

        if(aunt < 0) break;

        int parent_parent = ceil( (float)parent/ 2 ) - 1;
        int aunt_parent = ceil( (float)aunt/ 2 ) - 1;

        if(parent_parent == aunt_parent) {
            pthread_mutex_lock(&mutexArr[aunt]);
            while(treeArr[aunt] == -1) {
                pthread_cond_wait(&cvArr[aunt], &mutexArr[aunt]);
            }
            sum += treeArr[aunt];

            // pthread_cond_signal(&cvArr[aunt]);
            pthread_mutex_unlock(&mutexArr[aunt]);
        }

        curId = parent;
    }

    // cout << "thread_id: " << id << " sum: " << sum;
    for(int i = (leaf_id*block_size); i < (leaf_id*block_size) + n_ ; i++) {
        sum += arr[i];
        prefixSumArr[i] = sum;
        
    }

    return NULL;
 
}

void initializeTreeArr(int* treeArr) {
    for(int i=0;i < (2*NUM_LEAF_THREADS - 1); i++) {
        treeArr[i]=-1;
    }
}

void initializecvArr(pthread_cond_t* cvArr) {
    for(int i=0;i < (2*NUM_LEAF_THREADS - 1); i++) {
        pthread_cond_init(&cvArr[i], NULL);
    }
}

void initializeMutexArr(pthread_mutex_t* mutexArr) {
    for(int i=0;i < (2*NUM_LEAF_THREADS - 1); i++) {
        pthread_mutex_init(&mutexArr[i], NULL);
    }
}



int main() {
    int n = 10000;
    int* arr = new int[n];
    int* prefixSumArr = new int[n];
    int* treeArr = new int[2*NUM_LEAF_THREADS - 1];
    pthread_cond_t* cvArr = new pthread_cond_t[2*NUM_LEAF_THREADS - 1];
    pthread_mutex_t* mutexArr = new pthread_mutex_t[2*NUM_LEAF_THREADS - 1];
    pthread_t threads[2*NUM_LEAF_THREADS - 1];

    initializeTreeArr(treeArr);
    initializecvArr(cvArr);
    initializeMutexArr(mutexArr);

    for(int i=0;i<n;i++) {
        arr[i] = 1;
    }



    for(int i=0;i<(2*NUM_LEAF_THREADS - 1);i++) {

        prefixArgs* pArgs = new prefixArgs(arr, prefixSumArr, treeArr, cvArr, mutexArr, n, i);

        if(i<NUM_LEAF_THREADS-1) {
            pthread_create(&threads[i], NULL, prefixWorkerBody, (void*)pArgs);
        }
        else {
            pthread_create(&threads[i], NULL, prefixWorkerLeaf, (void*)pArgs);
        }
    }

    for(int i=0;i<(2*NUM_LEAF_THREADS - 1);i++) {

        pthread_join(threads[i], NULL);
    }

    // for(int i=0;i<n;i+=1) {
    //     cout << prefixSumArr[i] << " ";
    // }

    cout << endl;
     
}

