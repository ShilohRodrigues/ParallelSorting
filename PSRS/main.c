#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>

#define MASTER 0 // task ID of master task 

void swap(int* a, int* b);
int sequential_partition(int* data, int low, int high);
void sequential_quicksort(int* data, int low, int high);

int main(int argc, char *argv[]) {

  /** 
   * Start processes, get ranks and size 
  */
  int	  p_id,	     // process ID 
        p,         // number of processors 
        rc,        // return code 
        i, j;         // for loop counters

  int *data = NULL; // Holds the data to be sorted
  int size = 0;
  int capacity = 10; // Will grow as needed
  int *data_block = NULL; // Holds the data for each process (size = n/p)
  int block_size; // Size of data block
  
  MPI_Status status;

  MPI_Init(&argc,&argv);
  MPI_Comm_size(MPI_COMM_WORLD,&p);
  MPI_Comm_rank(MPI_COMM_WORLD,&p_id);
  if (p_id == MASTER) printf("%d MPI processes started...\n\n", p);

  /**
   * Master process reads input file
   * Check if there is an input file present and parse the data from the file
   * Check command line arg for file name, otherwise use default input.txt
   * Parses for space separated integers.
  */
  if (p_id == MASTER) {

    FILE *fp;

    // Attempt to open the file
    if ( argc != 2 )
      fp = fopen("../input.txt", "r");
    else
      fp = fopen(argv[1], "r");

    if (fp == NULL) { 
      MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Allocate initial memory to the data array
    data = (int *)malloc(capacity * sizeof(int));
    if (!data) {
      fclose(fp);
      MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Read the input file and parse for integers
    while (fscanf(fp, "%d", &data[size]) != EOF) {
      size++;
      if (size >= capacity) {
        // Double the capacity if needed
        capacity *= 2;
        data = (int *)realloc(data, capacity * sizeof(int));
        if (!data) {
            fclose(fp);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
      }
    }
    fclose(fp); // Close the file

    // Print the numbers to verify
    for(i = 0; i < size; i++) {
      printf("%d ", data[i]);
    }
    printf("\n");
    
  }

  // Set the size of the input data to each process, so they can calculate their block size
  MPI_Bcast(&size, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

  // Calculate the size of each data block
  // !!! Assumes n is divisible by p.. Need to account for when that isnt the case.
  block_size = size / p; 
  data_block = (int *)malloc(block_size * sizeof(int));
  if (!data_block) {
    printf("Unable to allocate memory for the data block.");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  
  // Scatter the data array to the other processes
  MPI_Scatter(data, block_size, MPI_INT, data_block, block_size, MPI_INT, MASTER, MPI_COMM_WORLD);

  // Free data block since it is separated amongst processes
  if (p_id == MASTER) free(data);

  // Each process sorts its own sublist sequentially
  sequential_quicksort(data_block, 0, block_size - 1);

  /**
   * Sort using PSRS.
   * 
   * Each process selects P items at the indices: 0, N/P2, 2N/P2,…, (P-1)N/P2
   * Master collects the samples and sorts them
   * Master chooses P-1 pivots at the indices P + P/2 -1, 2P + P/2 -1,…, (P-1)P + P/2 -1
   * Pivots are broadcasted to processes and each process partitions its local sorted sublists around the pivots.
   * Each proces sends its jth partition to Pj process, and keeps its own partition Pi
   * Each process merges partitions and combines results
   * 
  */

  int *regular_samples = (int *)malloc(p * sizeof(int));  
  int *master_regular_samples = NULL;
  int *pivots = (int *)malloc((p-1) * sizeof(int));;

  if (p_id == MASTER) master_regular_samples = (int *)malloc((p * p) * sizeof(int));

  // Choose regular samples
  for(i=0; i<p; i++) {
    int sample_i = (i*size)/(p*p);
    regular_samples[i] = data_block[sample_i];
  }

  // Send samples to the master process
  MPI_Gather(regular_samples, p, MPI_INT, master_regular_samples, p, MPI_INT, MASTER, MPI_COMM_WORLD);

  // Master sorts samples and chooses pivots
  if (p_id == MASTER) {

    sequential_quicksort(master_regular_samples, 0, ((p*p)-1));
    for(i=1; i<p; i++) {
      int pivot = (i*p) + (p/2) - 1;
      pivots[i-1] = master_regular_samples[pivot];
    }

    // Print samples for testing
    printf("\n"); 
    for(i = 0; i < (p*p); i++) {
      printf("%d ", master_regular_samples[i]);
    }
    printf("\n"); 

    free(master_regular_samples);

  }

  // Send pivots
  MPI_Bcast(pivots, (p-1), MPI_INT, MASTER, MPI_COMM_WORLD);

  // Print the pivots for testing
  if (p_id == MASTER) {
    printf("\n"); 
    for(i = 0; i < (p-1); i++) {
      printf("%d ", pivots[i]);
    }
    printf("\n"); 
  }
  
  // Partition the local sublists based on the received pivots
  int *send_counts = (int *)calloc(p, sizeof(int));
  int *send_displs = (int *)calloc(p, sizeof(int));
  int *recv_counts = (int *)malloc(p * sizeof(int));
  int *recv_displs = (int *)malloc(p * sizeof(int));

  // Calculate the send counts and displacements
  int current_pivot = 0;
  for (i = 0; i < block_size; i++) {
    if (current_pivot < (p-1) && data_block[i] > pivots[current_pivot]) {
        send_counts[current_pivot] = i - send_displs[current_pivot];
        current_pivot++;
        send_displs[current_pivot] = i;
    }
  }
  send_counts[current_pivot] = block_size - send_displs[current_pivot];

  // Communicate the send_counts to all processes to determine recv_counts
  MPI_Alltoall(send_counts, 1, MPI_INT, recv_counts, 1, MPI_INT, MPI_COMM_WORLD);

  // Calculate the receive displacements
  recv_displs[0] = 0;
  for (i = 1; i < p; i++) {
      recv_displs[i] = recv_displs[i - 1] + recv_counts[i - 1];
  }

  // Allocate memory for the receive buffer based on recv_counts
  int total_recv = recv_displs[p - 1] + recv_counts[p - 1];
  int *recv_data = (int *)malloc(total_recv * sizeof(int));

  // Perform the data exchange without MPI_IN_PLACE
  int *send_data = (int *)malloc(block_size * sizeof(int));
  memcpy(send_data, data_block, block_size * sizeof(int)); // Copy the local data to the send buffer

  MPI_Alltoallv(send_data, send_counts, send_displs, MPI_INT,
                recv_data, recv_counts, recv_displs, MPI_INT,
                MPI_COMM_WORLD);
  free(send_data);

  // All processes participate in MPI_Allgather to share their total_recv sizes
  MPI_Allgather(&total_recv, 1, MPI_INT, recv_counts, 1, MPI_INT, MPI_COMM_WORLD);

  // The recv_data now needs to be merged, but we'll assume that's done in a separate step
  int *gather_displs= NULL;
  int *sorted_data = NULL;
  if (p_id == MASTER) {

    gather_displs = (int *)malloc(p * sizeof(int));
    sorted_data = (int *)malloc(size * sizeof(int)); 

    gather_displs[0] = 0;
    for (i = 1; i < p; i++) {
        gather_displs[i] = gather_displs[i - 1] + recv_counts[i - 1];
    }

  }

  MPI_Gatherv(recv_data, total_recv, MPI_INT, 
            sorted_data, recv_counts, gather_displs, 
            MPI_INT, MASTER, MPI_COMM_WORLD);

  // Print the sorted array for verification
  if (p_id == MASTER) {
    printf("\n"); 
    for(i = 0; i < size; i++) {
      printf("%d ", sorted_data[i]);
    }
    printf("\n"); 
  }

  // Clean up
  free(send_counts);
  free(send_displs);
  free(recv_counts);
  free(recv_displs);
  free(recv_data);

  if (p_id == MASTER) {
    free(gather_displs);
    free(sorted_data); // Remember to free this only after the final sorted array is no longer needed
  }

  free(pivots);
  free(regular_samples);
  free(data_block);
  MPI_Finalize();

  return 0;

}

/**
 * Swaps two elements, passed by reference so that the original references are swapped
*/
void swap(int* a, int* b) {
  int temp = *a;
  *a = *b;
  *b = temp;
}

/**
 * Partitions the array using the rightmost index
*/
int partition(int* data, int low, int high, int pivot) {

  int i = (low - 1);
  int j;
  for(j = low; j < high; j++) {
    if(data[j] <= pivot) {
      i++;
      swap(&data[i], &data[j]);
    }
  }
  swap(&data[i+1], &data[high]);

  return (i+1);

}

/**
 * Recursive quicksort 
*/
void sequential_quicksort(int* data, int low, int high) {

  if (low >= high) return;

  int p = partition(data, low, high, data[high]);
  sequential_quicksort(data, low, p-1);
  sequential_quicksort(data, p+1, high);

}