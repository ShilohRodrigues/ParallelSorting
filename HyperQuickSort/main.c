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
        d,         // dimensions in hypercube
        i, j;      // for loop counter
    
  int *data = NULL; // Holds the data to be sorted
  int size = 0;
  int capacity = 10; // Will grow as needed
  int *data_block = NULL; // Holds the data for each process (size = n/p)
  int block_size;

  MPI_Status status;

  // Start separate tasks and get task ids 
  MPI_Init(&argc,&argv);
  MPI_Comm_size(MPI_COMM_WORLD,&p);
  MPI_Comm_rank(MPI_COMM_WORLD,&p_id);
  if (p_id == MASTER) printf("%d MPI processes started...\n\n", p);

  // Ensure that the number of processors is valid for a hypercube, i.e. powers of 2
  if ((p & (p - 1)) != 0) {
    printf("Number of processors must be a power of 2 for a hypercube topology."); 
    MPI_Abort(MPI_COMM_WORLD, 1);
  } 

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

  // Free data since it is separated amongst processes
  if (p_id == MASTER) free(data);

  /**
   * Hypercube quick sort:
   * Globally sorts the elements by exchanging between neighboring processes, 
   * then locally sorts using sequential quicksort,
   * finally, gathers all sub arrays into final sorted array.
   * Selects the rightmost index as pivot... Not a good pivot selection, done for simplicity for now..
  */

  // # of dimensions of the hcube equals log base 2 of # processors
  d = (int)(log(p) / log(2));

  // Loop for every dimension of the hypercube
  for (i = 0; i < d; i++) {

    int pivot;
    int neighbor = p_id ^ (1 << i);
    int *B1 = (int *)malloc(block_size * sizeof(int));
    int *B2 = (int *)malloc(block_size * sizeof(int));;
    int B1_size = 0, B2_size = 0;
    int *send_buffer = NULL;
    int *recv_buffer = NULL;
    int send_size;
    int recv_size;

    // Master process selects the pivot and broadcast to the other processes
    if (p_id == MASTER) pivot = data_block[block_size-1];
    MPI_Bcast(&pivot, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

    // Partition data_block into two blocks B1 <= pivot <= B2
    for (j = 0; j < block_size; j++) {
      if (data_block[j] <= pivot) {
        B1[B1_size++] = data_block[j];
      }
      else {
        B2[B2_size++] = data_block[j];
      }
    }

    // Check ith bit, determines if sending B1 or B2
    if ((p_id & (1 << i)) == 0) {
      send_buffer = B2;
      send_size = B2_size;
    }
    else {
      send_buffer = B1;
      send_size = B1_size;
    }

    // Notify neighbor how much data will be sent, and find out how much the neighbor is sending back
    MPI_Sendrecv(&send_size, 1, MPI_INT, neighbor, 0, &recv_size, 1, MPI_INT, neighbor, 0, MPI_COMM_WORLD, &status);

    // Prepare the receving buffer 
    recv_buffer = (int *)malloc(recv_size * sizeof(int));

    // Now send and receive B1 or B2
    MPI_Sendrecv(send_buffer, send_size, MPI_INT, neighbor, 0, recv_buffer, recv_size, MPI_INT, neighbor, 0, MPI_COMM_WORLD, &status);

    // Make data_block = B1|B2 concat recv_buffer
    free(data_block);
    if ((p_id & (1 << i)) == 0) {
      block_size = B1_size + recv_size; 
      data_block = (int *)malloc(block_size * sizeof(int));
      memcpy(data_block, B1, B1_size * sizeof(int));
      memcpy(data_block+B1_size, recv_buffer, recv_size * sizeof(int));
    }
    else {
      block_size = B2_size + recv_size; 
      data_block = (int *)malloc(block_size * sizeof(int));
      memcpy(data_block, B2, B2_size * sizeof(int));
      memcpy(data_block+B2_size, recv_buffer, recv_size * sizeof(int));
    }

  }

  // Sort data using sequential quicksort
  sequential_quicksort(data_block, 0, block_size-1);

  // Prepare for the final gather
  int *recv_counts = (int *)malloc(p * sizeof(int)); 
  int *recv_displs = (int *)malloc(p * sizeof(int)); 
  if (p_id == MASTER) data = (int *)malloc(size * sizeof(int));

  // Perform an Allgather, to get the number of elements each processor will return
  MPI_Allgather(&block_size, 1, MPI_INT, recv_counts, 1, MPI_INT, MPI_COMM_WORLD); 

  // Displacement values for the gatherv
  recv_displs[0] = 0;
  for (i = 1; i < p; i++) {
    recv_displs[i] = recv_displs[i - 1] + recv_counts[i - 1];
  }

  // Gathers all of the processors arrays into final sorted array
  MPI_Gatherv(data_block, block_size, MPI_INT, data, recv_counts, recv_displs, MPI_INT, MASTER, MPI_COMM_WORLD);
  
  // Print the sorted array to verify
  if (p_id == MASTER) {
    printf("\n"); 
    for(i = 0; i < size; i++) {
      printf("%d ", data[i]);
    }
    printf("\n"); 
  }

  // Clean up
  free(recv_counts);
  free(recv_displs);
  free(data_block);
  if (p_id == MASTER) free(data);
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
int sequential_partition(int* data, int low, int high) {

  int pivot = data[high];
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

  int p = sequential_partition(data, low, high);
  sequential_quicksort(data, low, p-1);
  sequential_quicksort(data, p+1, high);

}
