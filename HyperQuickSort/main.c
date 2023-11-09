#include "../SequentialQuickSort/quicksort.h"
#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <math.h>



#define MASTER 0 // task ID of master task 

int main(int argc, char *argv[]) {

  /**
   * Check if there is an input file present and parse the data from the file
   * Check command line arg for file name, otherwise use default input.txt
   * Parses for space separated integers.
  */
  FILE *fp;
  int *data = NULL; // Holds the data to be sorted
  int size = 0;
  int capacity = 10; // Will grow as needed

  // Attempt to open the file
  if ( argc != 2 )
    fp = fopen("input.txt", "r");
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
  for(int i = 0; i < size; i++) {
    printf("%d ", data[i]);
  }
  printf("\n");

  /**
   * Hypercube quick sort:
   * Selects the rightmost index as pivot... Not a good pivot selection, done for simplicity * for now..
  */
  int	  p_id,	     // process ID 
        p,         // number of processors 
        rc;        // return code 
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
  int d = (int)(log(p) / log(2));

  // Get the data block that each process will be assigned
  int block_size = (int)ceil(size / p);
  int *data_block = &data[p_id * block_size];

  // Loop for every dimension of the hypercube
  for (int i = 0; i < d; i++) {

    // Master process selects the pivot and broadcast to the other processes 
    int pivot = data_block[block_size-1];
    MPI_Bcast(&pivot, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

    // Get the id of the neighboring process along the ith dimension
    int neighbor_id = p_id ^ (1 << i); 

    // Partition data (D) such that D1 <= pivot < D2
    int partitionI = parallel_partition(data_block, 0, data_block_size - 1, pivot);

    // Calculate sizes of partitions to be sent and received
    int send_count = (p_id & (1 << i)) ? partition_index : block_size - partition_index;
    int *send_data = (p_id & (1 << i)) ? data_block : data_block + partition_index;
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

    // Allocate memory for the new data block
    free(data_block);
    data_block = (int *)malloc((send_count + recv_count) * sizeof(int));
    if (data_block == NULL) {
        perror("Unable to allocate memory for the merged data block.");
        free(data);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Merge the data sets into data_block
    merge_arrays(data_block, send_data, send_count, recv_data, recv_count);

    // Update 'block_size' to reflect the new size of 'data_block'
    block_size = send_count + recv_count;

    // Free the receive buffer
    free(recv_data);

  }

  // Sort data using sequential quicksort
  quicksort(data_block, 0, block_size-1)

  //Clean up
  free(data_block)
  MPI_Finalize();
  free(data);

  return 0;
}

int parallel_partition(int *data_block, int low, int high, int pivot) {

  int i = (low - 1);

  for (int j = low; j < high; j++) {
		if (data_block[j] <= pivot) {
			i++;
			int temp = data_block[i];
			data_block[i] = data_block[j];
			data_block[j] = temp;
		}
	}
	int temp = data_block[i + 1];
	data_block[i + 1] = data_block[hi];
	data_block[hi] = temp;

	return (data_block[i + 1] > pivot) ? i : (i + 1);

}

