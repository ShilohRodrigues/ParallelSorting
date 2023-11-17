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
  int *sorted_data = NULL; // Will hold the final sorted data

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

  // Print separated data blocks for testing
  MPI_Barrier(MPI_COMM_WORLD);
  for(i = 0; i < p; i++) {
    if (i == p_id) {
      printf("\nProcess %d: ", p_id);
      for(j = 0; j < block_size; j++) {
        printf("%d ", data_block[j]);
      }
      printf("\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);
  }

  /**
   * Hypercube quick sort:
   * Selects the rightmost index as pivot... Not a good pivot selection, done for simplicity for now..
  */

  // # of dimensions of the hcube equals log base 2 of # processors
  d = (int)(log(p) / log(2));

  // Loop for every dimension of the hypercube
  for (i = 0; i < d; i++) {

    int pivot;
    int partner = p_id ^ (1 << i);
    int *B1 = (int *)malloc(block_size * sizeof(int));
    int *B2 = (int *)malloc(block_size * sizeof(int));;
    int B1_size = 0, B2_size = 0;
    int *received_block = NULL;
    int received_size;

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

    // Prepare the buffers for sending and receiving
    int *send_buffer = (p_id & (1 << i)) ? B2 : B1;
    int send_size = (p_id & (1 << i)) ? B2_size : B1_size;

    int recv_size;
    // Probe to get the size of incoming data
    MPI_Probe(partner, 0, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_INT, &recv_size);
    int *recv_buffer = (int *)malloc(recv_size * sizeof(int));

    // Now send and receive using MPI_Sendrecv
    MPI_Sendrecv(send_buffer, send_size, MPI_INT, partner, 0, recv_buffer, recv_size, MPI_INT, partner, 0, MPI_COMM_WORLD, &status);

    // The data_block is now the received buffer
    free(data_block);
    data_block = recv_buffer;
    block_size = recv_size;

    // Prepare B1 and B2 for the next iteration if needed
    free(B1);
    free(B2);
    free(send_buffer);
    free(recv_buffer);


  }

  // Sort data using sequential quicksort
  sequential_quicksort(data_block, 0, block_size-1);

  // Gather all the processes data to the final sorted dataset
  // To do: Change to a gatherv, since the datablocks will have varied sizes after exchanges
  if (p_id == MASTER) {
    sorted_data = (int *)malloc(size * sizeof(int));
    if (!sorted_data) {
      printf("Error: Unable to allocate memory for the sorted data.\n");
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
  }
  MPI_Gather(data_block, block_size, MPI_INT, sorted_data, block_size, MPI_INT, MASTER, MPI_COMM_WORLD);
  
  // Print the sorted array to verify
  if (p_id == MASTER) {
    printf("\n"); 
    for(i = 0; i < size; i++) {
      printf("%d ", sorted_data[i]);
    }
    printf("\n"); 
    
  }

  // Clean up
  free(data_block);
  if (p_id == MASTER) free(sorted_data);
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
