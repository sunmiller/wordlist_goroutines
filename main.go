package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"sort"
)

var filePath string = "C:/temp/wordlist.txt"

type wordStats struct {
	length int
	count  int
}

type filePart struct {
	offset, size int64
}

var cpuCount int

func main() {
	fmt.Println("Hello, World!")
	cpuCount = runtime.NumCPU()
	fmt.Println("Number of CPUs:", cpuCount)
	totalLineCount, err := readthefile(filePath)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}
	fmt.Println("Total number of lines in the file:", totalLineCount)
	resultsCount, err := processTheFile(filePath)
	if err != nil {
		fmt.Println("Error processing file:", err)
		return
	}
	fmt.Println("Total number of unique words in the file:", resultsCount)

}

func processTheFile(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return 0, err
	}
	defer file.Close()

	results := make(map[string]wordStats)

	scanner := bufio.NewScanner(file)
	var line string
	for scanner.Scan() {
		line = scanner.Text()
		// fmt.Println(line)
		ts, ok := results[line]
		if ok {
			// fmt.Println("Found existing word:", line)
			ts.count++ // Increment the count for the existing word
		} else { // If the word is not in the map, add it with a count of 1
			ts = wordStats{count: 1} // Initialize the count to 1
			ts.length = len(line)    // Update the length of the existing word

		}
		results[line] = ts
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return 0, err
	}

	//sort the map alphabetically by key
	sortedKeys := make([]string, 0, len(results))
	for k := range results {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	// Print the sorted results
	// Print the results
	for _, key := range sortedKeys {
		fmt.Printf("Word: %s, Length: %d, Count: %d\n", key, results[key].length, results[key].count)
	}

	return len(results), nil

}

func readthefile(filePath string) ([]filePart, error) {

	fmt.Println("Reading file:", filePath)
	// Add your file reading logic here
	// You can use the "os" and "bufio" packages to read the file
	// For example, you can use os.Open() to open the file and read its contents
	// You can also use bufio.Scanner to read the file line by line
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	linesCount := 0
	for scanner.Scan() {
		linesCount++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	//get the number of equal parts in the file
	equalParts := (linesCount + cpuCount - 1) / cpuCount // Ceiling division in one step

	fileParts := make([]filePart, equalParts)

	for i := range fileParts {
		size := cpuCount
		if i == equalParts-1 { // Last chunk takes the remainder if it exists
			size = linesCount - i*cpuCount
		}
		fileParts[i] = filePart{offset: int64(i * cpuCount), size: int64(size)}
	}

	fmt.Println("Number of equal parts in the file:", equalParts)
	return fileParts, nil
}
