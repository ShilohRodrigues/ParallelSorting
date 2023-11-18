#include "s_quicksort.h"
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <string.h>

#define MASTER 0 // task ID of master task 

int parseFile(int **output, char *path, int *size);
void getRandomData(int *output, int size);

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
  int *data_block = NULL; // Holds the data for each process (size = n/p)
  int block_size;

  clock_t t; // To measure the sorting time

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
   * Master process checks cmd line arg, needs minimum 3 arguments.
   * 2nd argument says whether a file will be read or if random values will be used.
   * Check if there is an input file present and parse the data from the file
   * Otherwise create random values.
  */
  if (p_id == MASTER) {

    if (argc != 3) {
      perror("Two arguments must be stated. \n1st argument: 'file' or 'random', 2nd argument: 'file_path' or data_size");
      MPI_Abort(MPI_COMM_WORLD, 1);
    }

    if (strcmp(argv[1], "file") == 0) { // Parse the input file
      if (parseFile(&data, argv[2], &size) != 0) MPI_Abort(MPI_COMM_WORLD, 1);
    }  
    else if (strcmp(argv[1], "random") == 0) { // Populate data from random values
      size = atoi(argv[2]);
      data = (int *)malloc(size * sizeof(int));
      getRandomData(data, size);
    }
    else {
      perror("Two arguments must be stated. \n1st argument: 'file' or 'random', 2nd argument: 'file_path' or data_size");
      MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Print the numbers to verify
    printf("Input Size: %d \n", size);
    for(i = 0; i < size; i++) {
      printf("%d ", data[i]);
    }
    printf("\n");
    
  }

  // Start the sorting timer
  t = clock();

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
  quicksort(data_block, 0, block_size-1);

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

  // Stop the sorting timer
  t = clock() - t;
  double time_taken = (((double)t)/CLOCKS_PER_SEC)*1000; // in ms
  if (p_id == MASTER) printf("\n\nTime elapsed: %f\n", time_taken);

  // Clean up
  free(recv_counts);
  free(recv_displs);
  free(data_block);
  if (p_id == MASTER) free(data);
  MPI_Finalize();
  
  return 0;

}

/**
 * Parses a space separated input file into an array. 
*/
int parseFile(int **output, char *path, int *size) {

  int capacity = 10; // Will grow as needed

  FILE *fp;
  fp = fopen(path, "r");
  
  if (fp == NULL) return 1; // Could not open file

  // Allocate initial memory to the data array
  *output = (int *)malloc(capacity * sizeof(int));
  if (!*output) {
    fclose(fp);
    return 1;
  }

  *size = 0; // Initialize size to 0
  int value;
  // Read the input file and parse for integers
  while (fscanf(fp, "%d", &value) != EOF) {
    if (*size >= capacity) {
      // Double the capacity if needed
      capacity *= 2;
      int *new_output = (int *)realloc(*output, capacity * sizeof(int));
      if (!new_output) {
        free(*output);
        fclose(fp);
        return 1;
      }
      *output = new_output;
    }
    (*output)[*size] = value;
    (*size)++;
  }
  fclose(fp); // Close the file

  return 0;
}

void getRandomData(int *output, int size) {
  int i;
  // Seed the random number generator to get different results each time
  srand(time(NULL));
  for (i = 0; i < size; i++) {
      output[i] = rand() % 1000; // rand() % 1000 gives a range of 0 to 999
  }
}