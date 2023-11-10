#include "mpi.h"
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

  MPI_Status status;

  // Start separate tasks and get task ids 
  MPI_Init(&argc,&argv);
  MPI_Comm_size(MPI_COMM_WORLD,&p);
  MPI_Comm_rank(MPI_COMM_WORLD,&p_id);
  printf ("MPI task %d has started...\n", p_id);

  // Ensure that the number of processors is valid for a hypercube, i.e. powers of 2
  if ((p & (p - 1)) != 0) {
    printf("Number of processors must be a power of 2 for a hypercube topology."); 
    free(data);
    MPI_Abort(MPI_COMM_WORLD, 1);
  } 

  // # of dimensions of the hcube equals log base 2 of # processors
  d = (int)(log(p) / log(2));

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
      return 1; // Could not open the file
    }

    // Allocate initial memory to the data array
    data = (int *)malloc(capacity * sizeof(int));
    if (!data) {
      fclose(fp);
      return 1; // Could not allocate memory
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
            return 1; // Could not allocate memory
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
  block_size = (int)ceil((double)size / p);
  *data_block = (int *)malloc(block_size * sizeof(int));
  if (!data_block) {
    printf("Unable to allocate memory for the data block.");
    free(data);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  
  // Scatter the data array to the other processes
  MPI_Scatter(data, block_size, MPI_INT, data_block, block_size, MPI_INT, MASTER, MPI_COMM_WORLD);

  /**
   * Hypercube quick sort:
   * Selects the rightmost index as pivot... Not a good pivot selection, done for simplicity * for now..
  */
  int *merged_data = NULL;

  // Loop for every dimension of the hypercube
  for (i = 0; i < d; i++) {

    // Master process selects the pivot and broadcast to the other processes 
    int pivot = data_block[block_size-1];
    MPI_Bcast(&pivot, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

    // Get the id of the neighboring process along the ith dimension
    int neighbor_id = p_id ^ (1 << i); 

    // Partition data (D) such that D1 <= pivot < D2
    int partitionI = parallel_partition(data_block, 0, block_size - 1, pivot);

    // Calculate sizes of partitions to be sent and received
    int send_count = (p_id & (1 << i)) ? partitionI : block_size - partitionI;
    int *send_data = (p_id & (1 << i)) ? data_block : data_block + partitionI;
    int recv_count;

    // Exchange data sizes with the neighboring process
    MPI_Sendrecv(&send_count, 1, MPI_INT, neighbor_id, 0, 
                 &recv_count, 1, MPI_INT, neighbor_id, 0, 
                 MPI_COMM_WORLD, &status);

    // Allocate buffer for the incoming data
    int *recv_data = (int *)malloc(recv_count * sizeof(int));
    if (recv_data == NULL) {
        printf("Unable to allocate memory for receive buffer.");
        free(data);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Exchange the partitioned data with the neighboring process
    MPI_Sendrecv(send_data, send_count, MPI_INT, neighbor_id, 0, 
                 recv_data, recv_count, MPI_INT, neighbor_id, 0, 
                 MPI_COMM_WORLD, &status);

    // Allocate memory for merged data if needed
    if (merged_data == NULL) {
      merged_data = (int *)malloc((block_size * 2) * sizeof(int)); // Allocate enough memory
      if (merged_data == NULL) {
        printf("Unable to allocate memory for merged data.\n");
        free(data);
        MPI_Abort(MPI_COMM_WORLD, 1);
      }
    }

    // Merge the data sets into data_block
    merge_arrays(merged_data, send_data, send_count, recv_data, recv_count);

    // Reset arrays
    free(data_block); 
    data_block = merged_data; 
    merged_data = NULL;
    block_size = send_count + recv_count;

    // Finished with this array, since it was merged
    free(recv_data);

  }

  // Sort data using sequential quicksort
  sequential_quicksort(data_block, 0, block_size-1);

  //Clean up
  free(data_block);
  MPI_Finalize();
  free(data);

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
 * Merges two sorted arrays
*/
void merge_arrays(int *merged_data, int *send_data, int send_count, int *recv_data, int recv_count) {
    int i = 0, j = 0, k = 0;

    // Merge the two arrays until one is empty
    while (i < send_count && j < recv_count) {
        if (send_data[i] <= recv_data[j]) {
            merged_data[k++] = send_data[i++];
        } else {
            merged_data[k++] = recv_data[j++];
        }
    }

    // If send_data still has elements, add them to merged_data
    while (i < send_count) {
        merged_data[k++] = send_data[i++];
    }

    // If recv_data still has elements, add them to merged_data
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
