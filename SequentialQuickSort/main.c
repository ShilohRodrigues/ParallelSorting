#include <stdio.h>
#include <stdlib.h>

void quicksort(int* data, int low, int high);
int partition(int* data, int low, int high);
void swap(int* a, int* b);

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

  //Perform quicksort
  quicksort(data, 0, size-1);

  // Print sorted array
  for(int i = 0; i < size; i++) {
    printf("%d ", data[i]);
  }
  printf("\n");

  //Clean up
  fclose(fp);
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
 * Partitions the array using the rightmost index
*/
int partition(int* data, int low, int high) {

  int pivot = data[high];
  int i = (low - 1);

  for(int j = low; j < high; j++) {
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
void quicksort(int* data, int low, int high) {

  if (low >= high) return;

  int p = partition(data, low, high);
  quicksort(data, low, p-1);
  quicksort(data, p+1, high);

}