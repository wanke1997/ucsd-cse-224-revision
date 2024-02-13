package main

import (
	"fmt"
)

func merge(array1 []int, array2 []int) []int {
	pt1 := 0
	pt2 := 0
	pt := 0
	new_array := make([]int, len(array1)+len(array2))
	for pt1 < len(array1) || pt2 < len(array2) {
		if pt1 == len(array1) {
			new_array[pt] = array2[pt2]
			pt2 += 1
		} else if pt2 == len(array2) {
			new_array[pt] = array1[pt1]
			pt1 += 1
		} else {
			if array1[pt1] < array2[pt2] {
				new_array[pt] = array1[pt1]
				pt1 += 1
			} else {
				new_array[pt] = array2[pt2]
				pt2 += 1
			}
		}
		pt += 1
	}
	return new_array
}

func sort(array []int) []int {
	if len(array) <= 1 {
		return array
	} else {
		array1 := sort(array[:len(array)/2])
		array2 := sort(array[len(array)/2:])
		new_array := merge(array1, array2)
		return new_array
	}
}

func goroutine_sum(array []int, channel chan int) {
	sum := 0
	for _, num := range array {
		sum += num
	}
	channel <- sum
}

func main() {
	array := [12]int{3, 1, 2, -8, -12, 99, 23, 71, 2, -1, -88, 12}
	channel := make(chan int)
	sum := 0

	go goroutine_sum(array[:len(array)/2], channel)
	num := <-channel
	sum += num
	go goroutine_sum(array[len(array)/2:], channel)
	num = <-channel
	sum += num
	// res := sort(array[:])
	fmt.Println(sum)
}
