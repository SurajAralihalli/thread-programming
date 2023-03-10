#include <pthread.h>
#include <bits/stdc++.h>
#include <sys/time.h>
#include <stdlib.h>

using namespace std;

int* partitionArr;
int* arr;
int* prefixSumArr;


int get_lower_power_of_2(int x)
{
    // checks if x is power of 2 - 1
    int y = x + 1;
    if(y && (!(y & (y - 1)))) {
        return x + 1;
    }
    if (x == 0) {
        return 0;
    }
    x |= (x >> 1);
    x |= (x >> 2);
    x |= (x >> 4);
    x |= (x >> 8);
    x |= (x >> 16);
    return x - (x >> 1);
}

struct quickArgs {
    int low;
    int high;
    int num_threads;

    quickArgs(int l, int h, int num_threads ) : low(l), high(h), num_threads(num_threads) {}
};

struct partitionWorkerArgs {

    int* treeArr;
    pthread_cond_t* cvArr;
    pthread_mutex_t* mutexArr;
    int low;
    int n;
    int id;
    int pivot;
    bool partitionLeft;
    int partitionOffset;
    int num_leaf_threads;

    partitionWorkerArgs(int* tree, pthread_cond_t* c, pthread_mutex_t* m, int low, int n, int i, int pivot, bool pLeft, int pOffset, int num_leaf_threads) : treeArr(tree), cvArr(c), mutexArr(m), low(low), n(n), id(i), pivot(pivot), partitionLeft(pLeft), partitionOffset(pOffset), num_leaf_threads(num_leaf_threads) {}
};

struct partitionCopyArgs {

    int low;
    int n;
    int id;
    int num_threads;

    partitionCopyArgs(int low, int n, int i, int num_threads) : low(low), n(n), id(i), num_threads(num_threads) {}
};

int partitionSerial(int low, int high)
{
    int pivot = arr[low];

    int count = 0;
    for (int i = low + 1; i <= high; i++) {
        if (arr[i] <= pivot)
            count++;
    }

    int pivotIndex = low + count;
    swap(arr[pivotIndex], arr[low]);
 
    int i = low, j = high;

    while (i < pivotIndex && j > pivotIndex) {
 
        while (arr[i] <= pivot) {
            i++;
        }
 
        while (arr[j] > pivot) {
            j--;
        }
 
        if (i < pivotIndex && j > pivotIndex) {
            swap(arr[i++], arr[j--]);
        }
    }
 
    return pivotIndex;
}

void* partitionWorkerBody(void* arg) {
    partitionWorkerArgs* pArg = (partitionWorkerArgs*)arg;

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

void* partitionCopy(void* arg) {
    partitionCopyArgs* pArg = (partitionCopyArgs*)arg;


    int leaf_id = pArg->id;
    int low = pArg->low;
    int n = pArg->n;
    int num_threads = pArg->num_threads;

    int block_size = n / num_threads;
    int n_ = block_size;

    if(leaf_id == (num_threads-1)) {
        n_ += (n % num_threads);
    }


    for(int i = (leaf_id*block_size); i < (leaf_id*block_size) + n_ ; i++) {
        arr[low + i] = partitionArr[low + i];
    }

    return NULL;

}

void* partitionWorkerLeaf(void* arg) {
    partitionWorkerArgs* pArg = (partitionWorkerArgs*)arg;

    int* treeArr = pArg->treeArr;
    pthread_cond_t* cvArr = pArg->cvArr;
    pthread_mutex_t* mutexArr = pArg->mutexArr;
    int id = pArg->id;
    int low = pArg->low;
    int n = pArg->n;
    int pivot = pArg->pivot;
    bool partitionLeft = pArg->partitionLeft;
    int partitionOffset = pArg->partitionOffset;
    int num_leaf_threads = pArg->num_leaf_threads;


    int leaf_id = id + 1 - num_leaf_threads;
    int block_size = n / num_leaf_threads;
    int n_ = block_size;

    if(leaf_id == (num_leaf_threads-1)) {
        n_ += (n % num_leaf_threads);
    }

    int sum = 0;

    for(int i = low + (leaf_id*block_size); i < low + (leaf_id*block_size) + n_ ; i++) {
        sum += partitionLeft ? (arr[i] < pivot) : (arr[i] >= pivot);
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

    int prevSum = sum;
    for(int i = low + (leaf_id*block_size); i < low + (leaf_id*block_size) + n_ ; i++) {

        sum += partitionLeft ? (arr[i] < pivot) : (arr[i] >= pivot);
        prefixSumArr[i] = sum;

        if(prevSum != prefixSumArr[i]) {
            partitionArr[low + partitionOffset + prefixSumArr[i]-1] = arr[i];
        }

        prevSum = sum;
        
    }

    return NULL;
 
}

void initializeTreeArr(int* treeArr, int num_leaf_threads) {
    for(int i=0;i < (2*num_leaf_threads - 1); i++) {
        treeArr[i]=-1;
    }
}

void initializecvArr(pthread_cond_t* cvArr, int num_leaf_threads) {
    for(int i=0;i < (2*num_leaf_threads - 1); i++) {
        pthread_cond_init(&cvArr[i], NULL);
    }
}

void initializeMutexArr(pthread_mutex_t* mutexArr, int num_leaf_threads) {
    for(int i=0;i < (2*num_leaf_threads - 1); i++) {
        pthread_mutex_init(&mutexArr[i], NULL);
    }
}

int partitionParallel(int low, int high, int num_threads) {

    int num_tree_threads = get_lower_power_of_2(num_threads) - 1;
    int num_leaf_threads = ( get_lower_power_of_2(num_threads) / 2 );

    int n = high - low + 1;
    int partitionOffset = 0;
    bool partitionLeft = true;


    int* treeArr = new int[2*num_leaf_threads - 1];
    pthread_cond_t* cvArr = new pthread_cond_t[2*num_leaf_threads - 1];
    pthread_mutex_t* mutexArr = new pthread_mutex_t[2*num_leaf_threads - 1];
    pthread_t threads[num_threads];
    partitionWorkerArgs* workerArgs[2*num_leaf_threads - 1];

    partitionCopyArgs* copierArgs[num_threads];


    initializeTreeArr(treeArr, num_leaf_threads);
    initializecvArr(cvArr, num_leaf_threads);
    initializeMutexArr(mutexArr, num_leaf_threads);

    // partition left
    int pivot = arr[low];

    for(int i=0;i<(2*num_leaf_threads - 1);i++) {

        workerArgs[i] = new partitionWorkerArgs(treeArr, cvArr, mutexArr, low, n, i, pivot, partitionLeft, partitionOffset, num_leaf_threads);

        if(i<num_leaf_threads-1) {
            pthread_create(&threads[i], NULL, partitionWorkerBody, (void*)workerArgs[i]);
        }
        else {
            pthread_create(&threads[i], NULL, partitionWorkerLeaf, (void*)workerArgs[i]);
        }
    }

    for(int i=0;i<(2*num_leaf_threads - 1);i++) {
        pthread_join(threads[i], NULL);
        // delete heap variables
        delete workerArgs[i];
    }

    // partition right
    partitionLeft = false;
    partitionOffset = prefixSumArr[high];
    initializeTreeArr(treeArr, num_leaf_threads);


    for(int i=0;i<(2*num_leaf_threads - 1);i++) {

        workerArgs[i] = new partitionWorkerArgs(treeArr, cvArr, mutexArr, low, n, i, pivot, partitionLeft, partitionOffset, num_leaf_threads);

        if(i<num_leaf_threads-1) {
            pthread_create(&threads[i], NULL, partitionWorkerBody, (void*)workerArgs[i]);
        }
        else {
            pthread_create(&threads[i], NULL, partitionWorkerLeaf, (void*)workerArgs[i]);
        }
    }

    for(int i=0;i<(2*num_leaf_threads - 1);i++) {
        pthread_join(threads[i], NULL);
        // delete heap variables
        delete workerArgs[i];
    }

    //copy to arr
    for(int i=0;i<num_threads;i++) {
        copierArgs[i] = new partitionCopyArgs(low, n, i, num_threads);
        pthread_create(&threads[i], NULL, partitionCopy, (void*)copierArgs[i]);
    }

    for(int i=0;i<num_threads;i++) {
        pthread_join(threads[i], NULL);
        delete copierArgs[i];
    }

    // delete heap variables
    delete [] treeArr;
    delete [] cvArr;
    delete [] mutexArr;

    return (low + partitionOffset);

}


void quicksortSerial(int low, int high) {

    if(low < high) {
        int pi = partitionSerial(low, high);
        quicksortSerial(low, pi-1);
        quicksortSerial(pi+1, high);
    }
    
}

void* quickSortParallel(void* qArg) {
    quickArgs* q = (quickArgs*)qArg;
    int low = q->low;
    int high = q->high;
    int num_threads = q->num_threads;

    if (low < high) {

        if(high-low > 2500 && num_threads>=4) {

            int pi = partitionParallel(low, high, num_threads);

            int left_threads = ((pi-low) / (high - low)) * num_threads;

            quickArgs* t1Args = new quickArgs(low, pi-1, left_threads - 1);
            quickArgs* t2Args = new quickArgs(pi+1, high, num_threads - left_threads - 1);

            pthread_t t1, t2;
            
            pthread_create(&t1, NULL, quickSortParallel, (void*)t1Args);
            pthread_create(&t2, NULL, quickSortParallel, (void*)t2Args);

            pthread_join(t1, NULL);
            pthread_join(t2, NULL);

            delete t1Args;
            delete t2Args;


        }
        else {
            quicksortSerial(low, high);
        }

    }
    return NULL;

}


bool benchmark(int N, int num_threads) {
    arr = new int[N];
    prefixSumArr = new int[N];
    partitionArr = new int[N];
    
    srand(16);
    for(int i=0;i<N;i++) {
        arr[i] = rand();
    }

    struct timeval start_time;
    struct timeval end_time;
    struct timeval diff_time;

    
    quickArgs* qArgs = new quickArgs(0, N-1, num_threads);

    gettimeofday(&start_time, NULL);

    // call quick sort
    quickSortParallel((void*)qArgs);

    gettimeofday(&end_time, NULL);
    timersub(&end_time, &start_time, &diff_time);

    cout << "N:"<< N << ",num_threads:" << num_threads << ",sec:" << diff_time.tv_sec << ",microsec:" << diff_time.tv_usec << endl;

    bool test = true;
    int prev = arr[0];
    for(int i=1;i<N;i++) {
        if(arr[i]<prev) {
            test = false;
            break;
        }
        prev = arr[i];
    }

    delete [] arr;

    return test;

}

int main() {

    // vector<int> threads = {0,4,5,6,16,17,18,32,33,34,64,65,66};
    // for(int t: threads) {
    //     bool ret = benchmark(100000000, t);
    //     if(ret == false) {
    //         cout << "FAIL" << endl;
    //     }
    // }

    // for(int i=0; i<=100000000; i+=10000000) {
    //     bool ret = benchmark(i, 16);
    //     if(ret == false) {
    //         cout << "FAIL" << endl;
    //     }
    //     ret = benchmark(i, 32);
    //     if(ret == false) {
    //         cout << "FAIL" << endl;
    //     }
    // }

    benchmark(1000000, 16);
    benchmark(10000000, 16);
    benchmark(100000000, 16);

    return 0;
}
