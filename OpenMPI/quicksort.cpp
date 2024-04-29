#include <mpi.h>
#include <sys/time.h>
#include <bits/stdc++.h>

using namespace std;

int* arr;
int* l_arr;
int* u_arr;
int local_low;
int local_n;

//Declarations
int partitionSerial(int low, int high);
void quicksortSerial(int low, int high);
int mpi_partition(int low, int high, MPI_Comm mpi_comm);
void mpi_quicksort(int low, int high, MPI_Comm mpi_comm);
void synchronize_arr();

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

void quicksortSerial(int low, int high) {

    if(low < high) {
        int pi = partitionSerial(low, high);
        quicksortSerial(low, pi-1);
        quicksortSerial(pi+1, high);
    }
    
}

void selectionSort(int low, int high)
{
    int i,j,min_in;
    for(i=low;i<=high;i++)
    {
        min_in = i;
        for(j=i+1;j<=high;j++)
        {
            if (arr[j] < arr[min_in])
            {
                 min_in = j;
            }
        }
        swap(arr[i],arr[min_in]);
    }
}

void synchronize_arr() {

    int cluster_size;
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &cluster_size);
    int* bucket_counts = new int[cluster_size];
    int* displacement_counts = new int[cluster_size];

    MPI_Allgather(&local_low, 1, MPI_INT, displacement_counts, 1, MPI_INT, MPI_COMM_WORLD);
    MPI_Allgather(&local_n, 1, MPI_INT, bucket_counts, 1, MPI_INT, MPI_COMM_WORLD);

    MPI_Allgatherv(arr + local_low, local_n, MPI_INT, arr, bucket_counts, displacement_counts, MPI_INT, MPI_COMM_WORLD);

}

void mpi_quicksort(int low, int high, MPI_Comm mpi_comm) {

    if(low >= high) return;

    int rank, cluster_size;
    int n = high - low + 1;

    local_low = low;
    local_n = n;

    MPI_Comm_size(mpi_comm, &cluster_size);
    MPI_Comm_rank(mpi_comm, &rank);

    if(cluster_size <= 1 || n <= cluster_size) {

        quicksortSerial(low, high);
    }
    else {

        int pivot_index;

        pivot_index = mpi_partition(low, high, mpi_comm);

        int lhs_count = pivot_index - low;
        int rhs_count = high - pivot_index;
        // int num_left_processors = ((pivot_index - low) * cluster_size / (high - low));
        int num_left_processors = cluster_size/2;
        int num_right_processors = cluster_size - num_left_processors;

        if(lhs_count!=0 && num_left_processors==0) {
            num_left_processors++;
            num_right_processors--;
        }
        else if(rhs_count!=0 && num_right_processors==0)
        {
            num_left_processors--;
            num_right_processors++;
        }

        int first_right_rank = num_left_processors;

        int colour = int(rank < first_right_rank);

        MPI_Comm new_comm;
        MPI_Comm_split(mpi_comm, colour, rank, &new_comm);

        if(rank < first_right_rank) {

            int new_rank, cluster_size;
            MPI_Comm_size(new_comm, &cluster_size);
            MPI_Comm_rank(new_comm, &new_rank);
            mpi_quicksort(low, pivot_index, new_comm);
        }
        else {
            mpi_quicksort(pivot_index+1, high, new_comm);
        }

        MPI_Comm_free(&new_comm);
    }

    return;
}

int mpi_partition(int low, int high, MPI_Comm mpi_comm) {

    int rank, cluster_size;

    MPI_Comm_size(mpi_comm, &cluster_size);
    MPI_Comm_rank(mpi_comm, &rank);

    int partner_rank = cluster_size - rank - 1;

    int n = high - low + 1;
    int block_size = n / cluster_size;
    int n_ = block_size;

    if(rank == (cluster_size-1)) {
        n_ += (n % cluster_size);
    }

    int pivot = arr[low];

    int lcount = 0;
    int ucount = 0;

    int start = low;
    if(rank == 0) {
        start +=1; 
    }
    for(int i = start + (rank * block_size); i < low + (rank * block_size) + n_ ; i++) {
        if(arr[i] < pivot) {
            l_arr[lcount] = arr[i];
            lcount++;
        }
        else {
            u_arr[ucount] = arr[i];
            ucount++;
        }
    }

    int send_count;
    int recv_count;
    int bucket_count;

    // special case when cluster size is odd
    if(cluster_size%2!=0) {
        if(rank == (cluster_size/2)) {
            send_count = ucount;
            MPI_Send(&send_count, 1, MPI_INT, rank + 1, 1, mpi_comm);
            if(send_count!=0) {
                MPI_Send(u_arr, send_count, MPI_INT, rank + 1, 1, mpi_comm);
            }
        }
        else if(rank == (cluster_size/2) + 1) {
            MPI_Recv(&recv_count, 1, MPI_INT, rank - 1, 1, mpi_comm, MPI_STATUS_IGNORE);
            if(recv_count!=0) {
                MPI_Recv(u_arr + ucount, recv_count, MPI_INT, rank - 1, 1, mpi_comm, MPI_STATUS_IGNORE);
            }
            ucount += recv_count;
        }
    }

    if(rank < partner_rank) {
        //send
        send_count = ucount;
        MPI_Send(&send_count, 1, MPI_INT, partner_rank, 1, mpi_comm);
        if(send_count!=0) {
            MPI_Send(u_arr, send_count, MPI_INT, partner_rank, 1, mpi_comm);
        }

        //receive
        MPI_Recv(&recv_count, 1, MPI_INT, partner_rank, 1, mpi_comm, MPI_STATUS_IGNORE);
        if(recv_count!=0) {
            MPI_Recv(l_arr + lcount, recv_count, MPI_INT, partner_rank, 1, mpi_comm, MPI_STATUS_IGNORE);
        }
        bucket_count = lcount + recv_count;

    }
    else if(rank == partner_rank) {
        // handled previously
        bucket_count = lcount;
    }
    else {
        //receive
        MPI_Recv(&recv_count, 1, MPI_INT, partner_rank, 1, mpi_comm, MPI_STATUS_IGNORE);
        if(recv_count!=0) {
            MPI_Recv(u_arr + ucount, recv_count, MPI_INT, partner_rank, 1, mpi_comm, MPI_STATUS_IGNORE);
        }
        bucket_count = ucount + recv_count;

        //send
        send_count = lcount;
        MPI_Send(&send_count, 1, MPI_INT, partner_rank, 1, mpi_comm);
        if(send_count!=0) {
            MPI_Send(l_arr, send_count, MPI_INT, partner_rank, 1, mpi_comm);
        }
    }

    int pindex = (cluster_size%2==0) ? (cluster_size/2) :  ((cluster_size/2) + 1);

    //add pivot to correct bucket
    if(rank == (pindex-1)) {
        l_arr[bucket_count] = pivot;
        bucket_count++;
    }

    int* bucket_counts = new int[cluster_size];
    int* displacement_counts = new int[cluster_size];

    MPI_Allgather(&bucket_count, 1, MPI_INT, bucket_counts, 1, MPI_INT, mpi_comm);

    displacement_counts[0] = 0;
    for(int i=1; i<cluster_size; i++) {
        displacement_counts[i] = displacement_counts[i-1] + bucket_counts[i-1];
    }
    
    if(rank <= partner_rank) {
        MPI_Allgatherv(l_arr, bucket_count, MPI_INT, arr + low, bucket_counts, displacement_counts, MPI_INT, mpi_comm);
    }
    else {
        MPI_Allgatherv(u_arr, bucket_count, MPI_INT, arr + low, bucket_counts, displacement_counts, MPI_INT, mpi_comm);
    }

    int pivot_index = displacement_counts[pindex] + low - 1;
    return pivot_index;

}

void benchmark(int N, bool do_test, bool do_print) {

    int rank, cluster_size;
    unordered_map<int, int> org_arr_map;

    MPI_Comm_size(MPI_COMM_WORLD, &cluster_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    

    arr = new int[N];
    l_arr = new int[N];
    u_arr = new int[N];
    
    
    if(rank == 0) {
        srand(5);
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
    
    // call quick sort
    mpi_quicksort(0, N-1, MPI_COMM_WORLD);
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
    delete [] l_arr;
    delete [] u_arr;

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
