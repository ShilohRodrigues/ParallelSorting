#include "quicksort.h"

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