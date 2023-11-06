//#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>

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
   * Hyperquick sort
  */
 


  //Clean up
  fclose(fp);
  free(data);

  return 0;
}

