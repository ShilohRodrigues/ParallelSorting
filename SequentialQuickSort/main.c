#include "quicksort.h"
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>

int parseFile(int **output, char *path, int *size);
void getRandomData(int *output, int size);

int main(int argc, char *argv[]) {

  /**
   * Check if there is an input file present and parse the data from the file
   * Check command line arg for file name, otherwise use default input.txt
   * Parses for space separated integers.
  */
  int *data = NULL; // Holds the data to be sorted
  int size = 0;
  int i, j;

  if (argc != 3) {
    perror("Two arguments must be stated. \n1st argument: 'file' or 'random', 2nd argument: 'file_path' or data_size");
    return 1;
  }

  if (strcmp(argv[1], "file") == 0) { // Parse the input file
    if (parseFile(&data, argv[2], &size) != 0) return 1;
  }  
  else if (strcmp(argv[1], "random") == 0) { // Populate data from random values
    size = atoi(argv[2]);
    data = (int *)malloc(size * sizeof(int));
    getRandomData(data, size);
  }
  else {
    perror("Two arguments must be stated. \n1st argument: 'file' or 'random', 2nd argument: 'file_path' or data_size");
    return 1;
  }

  // Print the numbers to verify
  printf("Input Size: %d \n", size);
  for(i = 0; i < size; i++) {
    printf("%d ", data[i]);
  }
  printf("\n\n");

  //Perform quicksort
  quicksort(data, 0, size-1);

  // Print sorted array
  for(int i = 0; i < size; i++) {
    printf("%d ", data[i]);
  }
  printf("\n");

  //Clean up
  free(data);

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
