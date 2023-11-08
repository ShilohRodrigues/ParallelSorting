#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>

#define MASTER 0        /* task ID of master task */

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

  // Print the numbers to verify
  for(int i = 0; i < size; i++) {
    printf("%d ", data[i]);
  }
  printf("\n");

  /**
   * Hypercube quick sort
  */

  int	  taskid,	       /* task ID - also used as seed number */
        numtasks,      /* number of tasks */
        rc;            /* return code */
  MPI_Status status;

  // Start separate tasks and get task ids 
  MPI_Init(&argc,&argv);
  MPI_Comm_size(MPI_COMM_WORLD,&numtasks);
  MPI_Comm_rank(MPI_COMM_WORLD,&taskid);
  printf ("MPI task %d has started...\n", taskid);

  //Ensure that the number of processors is valid for a hypercube, i.e. powers of 2
  if ((numtasks & (numtasks - 1)) != 0) {
    printf("Number of processors must be a power of 2 for a hypercube topology.");
    fclose(fp);
    free(data);
    MPI_Abort(MPI_COMM_WORLD, 1);
  } 

  // Determine which section of data to sort, each task sorts n / p data

  // Loop for every dimension of the hypercube

    // Assign the pivot to partition around

    // Partition the data 

  // Sort data using sequential quicksort


  //Clean up
  fclose(fp);
  free(data);
  MPI_Finalize();

  return 0;
}

