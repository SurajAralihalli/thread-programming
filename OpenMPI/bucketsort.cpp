#include <mpi.h>
#include <sys/time.h>
#include <bits/stdc++.h>

using namespace std;

int* arr;
int* local_arr;
int* local_sorted_arr;
int* local_splitters;
int* all_splitters;
int local_n;

int partitionSerial(int low, int high);
void quicksortSerial(int low, int high);
void mpi_bucketsort(int start, int end);


int partitionSerial(int* arr, int low, int high)
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

void quicksortSerial(int* arr, int low, int high) {

    if(low < high) {
        int pi = partitionSerial(arr, low, high);
        quicksortSerial(arr, low, pi-1);
        quicksortSerial(arr, pi+1, high);
    }
    
}

void synchronize_arr() {

    int rank, cluster_size;

    MPI_Comm_size(MPI_COMM_WORLD, &cluster_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int* bucket_counts = new int[cluster_size];
    int* displacement_counts = new int[cluster_size];
    MPI_Allgather(&local_n, 1, MPI_INT, bucket_counts, 1, MPI_INT, MPI_COMM_WORLD);
    displacement_counts[0] = 0;
    for(int i=1; i<cluster_size; i++) {
        displacement_counts[i] = displacement_counts[i-1] + bucket_counts[i-1]; 
    }
    MPI_Allgatherv(local_sorted_arr, local_n, MPI_INT, arr, bucket_counts, displacement_counts, MPI_INT, MPI_COMM_WORLD);

    delete [] bucket_counts;
    delete [] displacement_counts;
}

void mpi_bucketsort(int start, int end) {

    int rank, cluster_size;

    MPI_Comm_size(MPI_COMM_WORLD, &cluster_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if(cluster_size==1) {
        quicksortSerial(arr, start, end);
        return;
    }

    int n = end - start + 1;

    int block_size = ceil(n / (double)cluster_size);
    

    int low = start + (rank * block_size);
    int high = min(low + block_size - 1, end);

    // sort locally
    quicksortSerial(arr, low, high);

    //find local splitters
    int step_size = ceil( (high - low + 1) / (double)(cluster_size) );
    
    int j = step_size;
    for(int i=0; i< cluster_size - 1; i++ ) {
        local_splitters[i] = arr[low + j - 1];
        j+=step_size;
    }

    //receive splitters from all processors
    MPI_Allgather(local_splitters, cluster_size-1, MPI_INT, all_splitters, cluster_size-1, MPI_INT, MPI_COMM_WORLD);

    //populate final splitters
    int all_splitters_count = (cluster_size * (cluster_size-1));
    quicksortSerial(all_splitters, 0,  all_splitters_count - 1);



    step_size = ( all_splitters_count / (cluster_size));
    j=step_size;
    for(int i=0; i< cluster_size - 1; i++ ) {
        local_splitters[i] = all_splitters[j-1];
        j+=step_size;
    }

    //send and receive personalized messages
    int* sendcnts = new int[cluster_size];
    int splitter_index = 0;
    int p = 0;
    int prev = 0;
    while(p < cluster_size-1) {
        int in = upper_bound(arr + low, arr + high + 1, local_splitters[splitter_index]) - (arr + low);
        sendcnts[p] = in - prev;
        prev = in;
        p++;
        splitter_index++;
    }

    // cout << "rank: " << rank << "prev: " << prev << " n: " << n << endl;
    sendcnts[p] = (high - low + 1) - prev;


    int* sdispls = new int[cluster_size];
    sdispls[0] = 0;
    for(int i=1; i<cluster_size; i++) {
        sdispls[i] = sdispls[i-1] + sendcnts[i-1]; 
    }

    int* recvcnts = new int[cluster_size];

    MPI_Alltoall(sendcnts, 1, MPI_INT, recvcnts, 1, MPI_INT, MPI_COMM_WORLD);

    int* rdispls = new int[cluster_size];
    rdispls[0] = 0;
    for(int i=1; i<cluster_size; i++) {
        rdispls[i] = rdispls[i-1] + recvcnts[i-1]; 
    }

    // sum of recvcnts
    for(int i=0; i < cluster_size; i++ ) {
        local_n += recvcnts[i];
    }

    MPI_Alltoallv(arr + low, sendcnts, sdispls, MPI_INT, local_arr, recvcnts, rdispls, MPI_INT, MPI_COMM_WORLD);

    int l_low = 0;
    int l_high = local_n - 1;

    // sort locally
    priority_queue<pair<int,int>,vector<pair<int,int>>,greater<pair<int,int>>> min_heap;
    for(int id = 0; id < cluster_size; id++)
    {
        if(recvcnts[id]>0)
        {
            int val = local_arr[rdispls[id]];
            min_heap.push({val,id});
            rdispls[id]++;
            recvcnts[id]--;
        }
    }
    int sarr_i = 0;
    while(!min_heap.empty()) {
        auto top = min_heap.top();
        min_heap.pop();
        int val = top.first;
        int id = top.second;

        local_sorted_arr[sarr_i] = val;
        sarr_i++;

        if(recvcnts[id]>0) {
            val = local_arr[rdispls[id]];
            min_heap.push({val,id});
            rdispls[id]++;
            recvcnts[id]--;
        }
    }


}

void benchmark(int N, bool do_test, bool do_print) {

    int rank, cluster_size;
    unordered_map<int, int> org_arr_map;

    MPI_Comm_size(MPI_COMM_WORLD, &cluster_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    
    arr = new int[N];
    local_arr = new int[N];
    local_sorted_arr = new int[N];
    local_splitters = new int[cluster_size -1];
    all_splitters = new int[cluster_size * (cluster_size-1)];
    local_n = 0;
    

    if(rank == 0) {
        srand(16);
        for(int i=0;i<N;i++) {
            arr[i] = rand();
            // arr[i] = 10;
            org_arr_map[arr[i]]++;
        }
    }

    MPI_Bcast(arr, N, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);


    if(rank == 0 && do_print) {
        cout << "Input: " << endl;
        for(int i=0;i<N;i++) {
            cout << arr[i] << " ";
        }
        cout << endl;
    }

    struct timeval start_time;
    struct timeval end_time;
    struct timeval diff_time;

    gettimeofday(&start_time, NULL);

    mpi_bucketsort(0, N-1);
    MPI_Barrier(MPI_COMM_WORLD);

    gettimeofday(&end_time, NULL);
    timersub(&end_time, &start_time, &diff_time);

    if(rank == 0 && !do_test) {
        cout << "N:"<< N << ",cluster_size:" << cluster_size << ",sec:" << diff_time.tv_sec << ",microsec:" << diff_time.tv_usec << endl;
    }

    // synchronize_arr
    if(cluster_size!=1) {
        synchronize_arr();
    }
    

    if(rank == 0 && do_print) {
        cout << "Output: " << endl;
        for(int i=0;i<N;i++) {
            cout << arr[i] << " ";
        }
        cout << endl;
    }
    
    if(rank == 0 && do_test) {

        cout << "N:"<< N << ",cluster_size:" << cluster_size << ",sec:" << diff_time.tv_sec << ",microsec:" << diff_time.tv_usec << endl;

        bool test = true;
        int prev = arr[0];
        for(int i=1;i<N;i++) {
            if(arr[i]<prev) {
                test = false;
                // cout << "FAIL at " << i << endl; 
                break;
            }
            prev = arr[i];
        }

        for(int i = 0; i < N; i++) {
            int ele = arr[i];
            if(org_arr_map.find(ele) == org_arr_map.end()) {
                test = false;
                break;
            }
            org_arr_map[ele]--;
            if(org_arr_map[ele] < 0) {
                test = false;
                break;
            }
        }

        for(auto it = org_arr_map.begin(); it != org_arr_map.end(); it++) {
            if(it->second != 0) {
                test = false;
                break;
            }
        }

        if(!test) {
            cout << "## TEST FAIL ##" << endl;
        }
        else {
            cout << "## TEST PASS ##" << endl;
        }
    }
    
    delete [] arr;
    delete [] local_arr;
    delete [] local_sorted_arr;
    delete [] local_splitters;
    delete [] all_splitters;

    return;

}

int main(int argc, char** argv) {
    
    MPI_Init(&argc, &argv);
    bool do_test = false;
    bool do_print = false;

    // vector<int> tests = {1000, 10000, 100000, 1000000, 2000000, 3000000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000};
    // vector<int> tests = {100000, 1000000, 10000000};

    int N = 100000000;
    if(argc == 2) {
        N = atoi(argv[1]);
    }
    // for(int m : tests) {
    //     benchmark(m, do_test, do_print);
    // }

    benchmark(N, do_test, do_print);

    MPI_Finalize();

    return 0;
}
