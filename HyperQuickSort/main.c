#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>

#define MASTER 0 // task ID of master task 

void swap(int* a, int* b);
int parallel_partition(int *data_block, int low, int high, int pivot);
void merge_arrays(int *merged_data, int *send_data, int send_count, int *recv_data, int recv_count);
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
        i;         // for loop counter
    
  int *data = NULL; // Holds the data to be sorted
  int size = 0;
  int capacity = 10; // Will grow as needed
  int *data_block = NULL; // Holds the data for each process (size = n/p)
  int block_size;
  int *sorted_data = NULL;

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

  // Free data block since it is separated amongst processes
  if (p_id == MASTER) free(data);

  /**
   * Hypercube quick sort:
   * Selects the rightmost index as pivot... Not a good pivot selection, done for simplicity * for now..
  */

  // # of dimensions of the hcube equals log base 2 of # processors
  d = (int)(log(p) / log(2));

  // Loop for every dimension of the hypercube
  for (i = 0; i < d; i++) {

    int neighbor_id = p_id ^ (1 << i);
    int *recv_data = NULL;
    int recv_data_size = 0;

    // Master process selects the pivot and broadcast to the other processes
    int pivot = data_block[block_size-1];
    MPI_Bcast(&pivot, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

    // Partition data (D) such that D1 <= pivot < D2
    int partition_index = parallel_partition(data_block, 0, block_size - 1, pivot);

    // Determine the size of data to send and receive
    int send_data_size = (p_id & (1 << i)) ? partition_index : (block_size - partition_index);
    int *send_data = (p_id & (1 << i)) ? data_block : (data_block + partition_index);

    // Allocate buffer for the incoming data
    MPI_Sendrecv(&send_data_size, 1, MPI_INT, neighbor_id, 0, &recv_data_size, 1, MPI_INT, neighbor_id, 0, MPI_COMM_WORLD, &status);

    recv_data = (int *)malloc(recv_data_size * sizeof(int));
    if (recv_data == NULL) {
        fprintf(stderr, "Unable to allocate memory for receive buffer.\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Exchange the data
    MPI_Sendrecv(send_data, send_data_size, MPI_INT, neighbor_id, 0, recv_data, recv_data_size, MPI_INT, neighbor_id, 0, MPI_COMM_WORLD, &status);

    int *merge_data = NULL;
    //int merge_data_size = 

    free(recv_data);
    
    // int neighbor_id;
    // int *new_data, *send_data, *recv_data, *merge_data;
    // int new_data_size, send_data_size, recv_data_size, merge_data_size;
    // int mask = p;
    // int pivot;

    // // Master process selects the pivot and broadcast to the other processes 
    // pivot = data_block[block_size-1];
    // MPI_Bcast(&pivot, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

    // // Partition data (D) such that D1 <= pivot < D2
    // int partitionI = parallel_partition(data_block, 0, block_size - 1, pivot);

    // // Checks if the ith bit is 0
    // mask = mask >> 1;
    // if ((p_id & mask) == 0) {

    //   neighbor_id = p_id + mask;
    //   send_data = &data_block[partitionI + 1];
    //   send_data_size = block_size - partitionI - 1;
    //   if (send_data_size < 0) send_data_size = 0;

    //   new_data = &data_block[0];
		// 	new_data_size = partitionI + 1;

    // }
    // else {

    //   neighbor_id = p_id - mask;
    //   send_data = &data_block[0];
    //   send_data_size = partitionI + 1;
    //   if (send_data_size > block_size) send_data_size = partitionI;

    //   new_data = &data_block[partitionI + 1];
		// 	new_data_size = block_size - partitionI - 1;
		// 	if (new_data_size < 0) new_data_size = 0;

    // }

    // MPI_Sendrecv(&send_data_size, 1, MPI_INT, neighbor_id, 0, &recv_data_size, 1, MPI_INT, neighbor_id, 0, MPI_COMM_WORLD, &status);

    // recv_data = (int *)malloc(recv_data_size * sizeof(int));
    // MPI_Sendrecv(send_data, send_data_size, MPI_INT, neighbor_id, 0, recv_data, recv_data_size, MPI_INT, neighbor_id, 0, MPI_COMM_WORLD, &status);

    // merge_data_size = new_data_size + recv_data_size;
    // merge_data = (int *)malloc(merge_data_size * sizeof(int));
    // merge_arrays(merge_data, send_data, send_data_size, recv_data, recv_data_size);

    // data_block = merge_data;
    // block_size = merge_data_size;

    // free(recv_data);

  }

  // Sort data using sequential quicksort
  sequential_quicksort(data_block, 0, block_size-1);

  MPI_Barrier(MPI_COMM_WORLD);
  for(i = 0; i < p; i++) {
    if (i == p_id) {
      int j;
      printf("\nProcess %d: ", p_id);
      for(j = 0; j < block_size; j++) {
        printf("%d ", data_block[j]);
      }
      printf("\n");
    }
    MPI_Barrier(MPI_COMM_WORLD);
  }

  // Gather all the processes data to the final sorted dataset
  if (p_id == MASTER) {
    sorted_data = (int *)malloc(size * sizeof(int));
    if (!sorted_data) {
      printf("Error: Unable to allocate memory for the sorted data.\n");
      MPI_Abort(MPI_COMM_WORLD, 1);
    }
  }
  MPI_Gather(data_block, 1, MPI_INT, sorted_data, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
  
  // Print the sorted array to verify
  if (p_id == MASTER) {
    printf("\n"); 
    for(i = 0; i < size; i++) {
      printf("%d ", sorted_data[i]);
    }
    printf("\n"); 
    free(sorted_data);
  }

  // Clean up
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
 * Parallel partitioning function
*/
int parallel_partition(int *data_block, int low, int high, int pivot) {

  int i = (low - 1);
  int j;

  for (j = low; j < high; j++) {
		if (data_block[j] <= pivot) {
			i++;
			swap(&data_block[i], &data_block[j]);
		}
	}
	swap(&data_block[i+1], &data_block[high]);

	return (data_block[i + 1] > pivot) ? i : (i + 1);

}

/**
 * Merges two arrays
*/
void merge_arrays(int *merged_data, int *send_data, int send_count, int *recv_data, int recv_count) {
    int i = 0, j = 0, k = 0;

    while (i < send_count && j < recv_count) {
        if (send_data[i] < recv_data[j]) {
            merged_data[k++] = send_data[i++];
        } else {
            merged_data[k++] = recv_data[j++];
        }
    }
    while (i < send_count) {
        merged_data[k++] = send_data[i++];
    }
    while (j < recv_count) {
        merged_data[k++] = recv_data[j++];
    }
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
