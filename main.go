package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
)

var filePath string = "C:/temp/wordlist.txt"

// var filePath string = "C:/temp/smallwordlist.txt"

type wordStats struct {
	length int
	count  int
}

type filePart struct {
	offset, size int64
}

// var cpuCount int

var maxGoroutines int

func main() {
	goroutines := flag.Int("goroutines", 0, "num goroutines for parallel solutions (default NumCPU)")
	output := bufio.NewWriter(os.Stdout)
	maxGoroutines = *goroutines
	if maxGoroutines == 0 {
		maxGoroutines = runtime.NumCPU()
	}
	fmt.Printf("Number of goroutines %d \n", maxGoroutines)

	// totalfileParts, err := readthefile(filePath, maxGoroutines)
	totalfileParts, err := splitFileGivenTheNumberOfParts(filePath, maxGoroutines)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}

	resultsCh := make(chan map[string]wordStats)
	for _, filePart := range totalfileParts {
		go processPartOfTheFile(filePath, filePart.offset, filePart.size, resultsCh)
	}

	finalResults := make(map[string]wordStats)

	for range totalfileParts {
		wordResultsFromGoRoutine := <-resultsCh

		for word, wordStatsFromGoRoutine := range wordResultsFromGoRoutine {
			foundWordStats, ok := finalResults[word]
			if !ok {
				finalResults[word] = wordStats{
					length: wordStatsFromGoRoutine.length,
					count:  wordStatsFromGoRoutine.count,
				}
				continue
			}
			// fmt.Printf("found the word %s \n", word)
			foundWordStats.count = foundWordStats.count + wordStatsFromGoRoutine.count
			finalResults[word] = foundWordStats
		}
	}

	// sort the map alphabetically by key
	sortedKeys := make([]string, 0, len(finalResults))
	for k := range finalResults {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	// Print the sorted results

	for _, word := range sortedKeys {

		foundWordStats := finalResults[word]
		fmt.Fprintf(output, "Word: %s, Length: %d, Count: %d\n", word, foundWordStats.length, foundWordStats.count)

	}

	output.Flush()
}

// func processTheFile(filePath string) (int, error) {

// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		fmt.Println("Error opening file:", err)
// 		return 0, err
// 	}
// 	defer file.Close()

// 	results := make(map[string]wordStats)

// 	scanner := bufio.NewScanner(file)
// 	var line string
// 	for scanner.Scan() {
// 		line = scanner.Text()
// 		// fmt.Println(line)
// 		ts, ok := results[line]
// 		if ok {
// 			// fmt.Println("Found existing word:", line)
// 			ts.count++ // Increment the count for the existing word
// 		} else { // If the word is not in the map, add it with a count of 1
// 			ts = wordStats{count: 1} // Initialize the count to 1
// 			ts.length = len(line)    // Update the length of the existing word

// 		}
// 		results[line] = ts
// 	}

// 	if err := scanner.Err(); err != nil {
// 		fmt.Println("Error reading file:", err)
// 		return 0, err
// 	}

// 	//sort the map alphabetically by key
// 	sortedKeys := make([]string, 0, len(results))
// 	for k := range results {
// 		sortedKeys = append(sortedKeys, k)
// 	}
// 	sort.Strings(sortedKeys)
// 	// Print the sorted results
// 	// Print the results
// 	for _, key := range sortedKeys {
// 		fmt.Printf("Word: %s, Length: %d, Count: %d\n", key, results[key].length, results[key].count)
// 	}

// 	return len(results), nil

// }

func processPartOfTheFile(filePath string, lineOffset int64, lineSize int64, resultsCh chan map[string]wordStats) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	_, err = file.Seek(lineOffset, io.SeekStart)
	if err != nil {
		panic(err)
	}
	f := io.LimitedReader{R: file, N: lineSize}

	results := make(map[string]wordStats)

	scanner := bufio.NewScanner(&f)

	for scanner.Scan() {

		line := scanner.Text()
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

	resultsCh <- results
}

// func readthefile(filePath string, maxGoroutines int) ([]filePart, error) {

// 	fmt.Println("Reading file:", filePath)
// 	// Add your file reading logic here
// 	// You can use the "os" and "bufio" packages to read the file
// 	// For example, you can use os.Open() to open the file and read its contents
// 	// You can also use bufio.Scanner to read the file line by line
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		fmt.Println("Error opening file:", err)
// 		return nil, err
// 	}
// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)
// 	linesCount := 0
// 	for scanner.Scan() {
// 		linesCount++
// 	}
// 	if err := scanner.Err(); err != nil {
// 		return nil, err
// 	}
// 	fmt.Println("Total number of lines in the file:", linesCount)
// 	//get the number of equal parts in the file
// 	equalParts := (linesCount + maxGoroutines - 1) / maxGoroutines // Ceiling division in one step

// 	fileParts := make([]filePart, equalParts)

// 	for i := range fileParts {
// 		size := maxGoroutines
// 		if i == equalParts-1 { // Last chunk takes the remainder if it exists
// 			size = linesCount - i*maxGoroutines
// 		}
// 		fileParts[i] = filePart{offset: int64(i * maxGoroutines), size: int64(size)}
// 	}

// 	fmt.Println("Number of equal parts in the file:", equalParts)
// 	return fileParts, nil
// }

func splitFileGivenTheNumberOfParts(inputpath string, numParts int) ([]filePart, error) {
	const maxLineLength = 100

	// open the file
	f, err := os.Open(inputpath)
	if err != nil {
		return nil, err
	}

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	// It gets the file's total size (in bytes).
	size := st.Size()
	splitSize := size / int64(numParts)

	fmt.Printf("Size of the file %d \n", size)
	fmt.Printf("Split size %d \n", splitSize)
	// buf is a small buffer (100 bytes) to help find line breaks.
	buf := make([]byte, maxLineLength)
	// partsList is a list that will store all the split partsList.
	partsList := make([]filePart, 0, numParts)
	// offset is the current position in the file.
	offset := int64(0)
	// This loop will iterate over the file and find the split points.
	for i := range numParts {

		// If it's the last part, just take the rest of the file
		if i == numParts-1 {
			if offset < size {
				partsList = append(partsList, filePart{offset, size - offset})
			}
			break
		}
		// We want to avoid cutting a line in half, so we move a little back and look for a newline (\n).
		seekOffset := max(offset+splitSize-maxLineLength, 0)
		// is moving the file read position to a specific location (seekOffset) before reading data.
		_, err := f.Seek(seekOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		// reads a fixed amount of data from the file into the buf buffer.
		n, _ := io.ReadFull(f, buf)
		// creates a new slice (chunk) that contains only the bytes that were actually read from the file.
		chunk := buf[:n]
		// finds the last newline character in the chunk.
		newline := bytes.LastIndexByte(chunk, '\n')
		if newline < 0 {
			return nil, fmt.Errorf("newline not found at offset %d", offset+splitSize-maxLineLength)
		}
		// calculates the remaining bytes in the chunk.
		remaining := len(chunk) - newline - 1
		// calculates the next offset.
		nextOffset := seekOffset + int64(len(chunk)) - int64(remaining)
		// appends the part to the partsList list.
		partsList = append(partsList, filePart{offset, nextOffset - offset})
		// updates the offset to the next offset.
		offset = nextOffset
	}
	return partsList, nil
}
