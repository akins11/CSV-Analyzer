package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Dataset represent our CSV data structure
type Dataset struct {
	Headers     []string
	Rows        [][]string
	NumericCols map[int]bool // track which columns are numeric
}

// ColumnStats holds statistical information for a column
type ColumnStats struct {
	Name   string
	Count  int
	Sum    float64
	Mean   float64
	Median float64
	StdDev float64
	Min    float64
	Max    float64
}

// CSVAnalyzer handles the analysis operations
type CSVAnalyzer struct {
	dataset *Dataset
}

// NewCSVAnalyzer creates a new analyzer instance
func NewCSVAnalyzer() *CSVAnalyzer {
	return &CSVAnalyzer{
		dataset: &Dataset{
			NumericCols: make(map[int]bool),
		},
	}
}

// LoadCSV reads and parses a CSV file
// Defines a method named 'LoadCSV' for CSVAnalyzer, taking a filename string and returning an error.
func (ca *CSVAnalyzer) LoadCSV(filename string) error {
	// Attempts to open the file specified by 'filename'. Returns a file object and an error (if any).
	file, err := os.Open(filename)
	// Checks if an error occurred during file opening.
	if err != nil {
		// If there's an error, wraps it with a descriptive message and returns it.
		return fmt.Errorf("error opening file: %v", err)
	}
	// Ensures the file is closed when the function exits, regardless of how it exits.
	defer file.Close()
	// Creates a new CSV reader that will read from the opened file.
	reader := csv.NewReader(file)
	// Reads all available CSV records from the reader into a slice of string slices.
	records, err := reader.ReadAll()
	// Checks if an error occurred during CSV reading.
	if err != nil {
		// If an error, wraps it with a message and returns it.
		return fmt.Errorf("error reading CSV file: %v", err)
	}
	// Checks if no records were read, indicating an empty CSV file.
	if len(records) == 0 {
		// If empty, returns an error message.
		return fmt.Errorf("empty csv file")
	}

	// First row is headers
	// Assigns the first row of records as the dataset's headers.
	ca.dataset.Headers = records[0]
	// Assigns all subsequent rows (from the second row onwards) as the dataset's data rows.
	ca.dataset.Rows = records[1:]

	// Detect numeric columns
	// Calls the 'detectNumericColumns' method to identify numeric columns in the loaded data.
	ca.detectNumericColumns()
	// If all operations are successful, returns nil, indicating no error.
	return nil
}

// detectNumericColumns identifies which columns contain numeric data
// This detectNumericColumns function is a method of the CSVAnalyzer type. Its primary purpose is to examine the data within a
// CSV dataset and identify which columns contain predominantly numeric values. It does this by iterating through each column
// and checking the first few rows to see if the values in that column can be successfully converted to a floating-point number.
// Defines a method named 'detectNumericColumns' that operates on a pointer to a CSVAnalyzer struct.
func (ca *CSVAnalyzer) detectNumericColumns() {
	// Checks if the dataset has no rows.
	if len(ca.dataset.Rows) == 0 {
		// If there are no rows, exit the function.
		return
	}

	// Loop through each column based on the number of headers.
	for colIndex := range ca.dataset.Headers {
		// Initialize a flag for the current column, assuming it's numeric until proven otherwise.
		isNumeric := true

		// Check first few rows to determine if column is numeric
		// checkRows := min(len(ca.dataset.Rows), 10)
		checkRows := len(ca.dataset.Rows)
		if checkRows > 10 {
			checkRows = 10 // Limit to first 10 rows for numeric check
		}
		// Loop through the determined number of rows.
		for rowIndex := 0; rowIndex < checkRows; rowIndex++ {
			// Ensures the column index is within the bounds of the current row's data.
			if colIndex < len(ca.dataset.Rows[rowIndex]) {
				// Get the cell value and remove leading/trailing whitespace.
				value := strings.TrimSpace(ca.dataset.Rows[rowIndex][colIndex])
				// Check if the trimmed value is not empty.
				if value != "" {
					// Attempt to convert the value to a float64; if an error occurs, it's not numeric.
					if _, err := strconv.ParseFloat(value, 64); err != nil {
						// Set the flag to false, indicating the column is not numeric.
						isNumeric = false
						// Stop checking this column as it's already identified as non-numeric.
						break
					}
				}
			}
		}
		// Store the result (whether the column is numeric) in the dataset's map.
		ca.dataset.NumericCols[colIndex] = isNumeric
	}
}

// CalculateStats computes statistics for numeric columns
// The CalculateStats method is part of the CSVAnalyzer struct. Its main goal is to compute various statistical measures
// (like sum, mean, median, standard deviation, min, and max) for each numeric column in the loaded CSV dataset.
// It iterates through the identified numeric columns, extracts their numeric values, calculates the statistics, and
// then compiles these statistics into a slice of ColumnStats structs, which it returns.
// Defines a method 'CalculateStats' for CSVAnalyzer, returning a slice of ColumnStats structs.
func (ca *CSVAnalyzer) CalculateStats() []ColumnStats {
	// Declares an empty slice named 'stats' to store the calculated statistics for each column.
	var stats []ColumnStats
	// Iterates through the map of numeric columns (colIndex is the column index, isNumeric is a boolean indicating if it's numeric).
	for colIndex, isNumeric := range ca.dataset.NumericCols {
		// Checks if the column is NOT numeric OR if its index is out of bounds for the headers.
		if !isNumeric || colIndex >= len(ca.dataset.Headers) {
			// If either condition is true, skip to the next column.
			continue
		}
		// Calls a helper method to extract all numeric values from the current column.
		values := ca.extractNumericValues(colIndex)
		// Checks if no numeric values were successfully extracted from the column.
		if len(values) == 0 {
			// If the column has no valid numeric values, skip to the next column.
			continue
		}
		// Creates a new instance of the 'ColumnStats' struct.
		colStats := ColumnStats{
			// Assigns the column header as the name for these statistics.
			Name: ca.dataset.Headers[colIndex],
			// Records the number of valid numeric values found in the column.
			Count: len(values),
		}

		// Calculate basic stats
		// Calls a 'sum' utility function to calculate the sum of all numeric values.
		colStats.Sum = sum(values)
		// Calculates the mean (average) by dividing the sum by the count of values.
		colStats.Mean = colStats.Sum / float64(len(values))
		// Calls a 'median' utility function to calculate the median of the values.
		colStats.Median = median(values)
		// Calls a 'standardDeviation' utility function to calculate the standard deviation using the values and their mean.
		colStats.StdDev = standardDeviation(values, colStats.Mean)
		// Calls a 'min' utility function to find the minimum value in the slice (using variadic arguments).
		colStats.Min = min(values...)
		// Calls a 'max' utility function to find the maximum value in the slice (using variadic arguments).
		colStats.Max = max(values...)
		// Appends the populated 'colStats' struct to the 'stats' slice.
		stats = append(stats, colStats)
	}
	// Returns the slice containing statistics for all identified numeric columns.
	return stats
}

// The extractNumericValues method is a helper function belonging to the CSVAnalyzer struct. Its sole purpose is to iterate
// through a specific column of the loaded CSV data, attempt to convert each cell's value in that column into a float64, and
// collect all successfully converted numeric values into a new slice of float64. This function ensures that only valid numeric
// data is used for subsequent statistical calculations.
// extractNumericValues gets all numeric values from a column
// Defines a method 'extractNumericValues' for CSVAnalyzer, taking a column index (int) and returning a slice of float64s.
func (ca *CSVAnalyzer) extractNumericValues(colIndex int) []float64 {
	// Declares an empty slice of float64s named 'values' to store extracted numeric data.
	var values []float64
	// Iterates through each 'row' in the dataset's 'Rows' (which are slices of strings).
	for _, row := range ca.dataset.Rows {
		// Checks if the 'colIndex' is valid for the current 'row' (i.e., the row has data at that column).
		if colIndex < len(row) {
			// Extracts the string value from the specified column in the current row and removes leading/trailing whitespace.
			value := strings.TrimSpace(row[colIndex])
			// Checks if the trimmed string 'value' is not empty.
			if value != "" {
				// Attempts to parse the string 'value' into a float64. Checks if the parsing was successful (err is nil).
				if num, err := strconv.ParseFloat(value, 64); err == nil {
					// If parsing is successful, appends the converted float64 'num' to the 'values' slice.
					values = append(values, num)
				}
			}
		}
	}
	// Returns the slice containing all successfully extracted numeric values from the column.
	return values
}

// Statistical functions
func sum(values []float64) float64 {
	total := 0.0
	for _, v := range values {
		total += v
	}
	return total
}

// The median function calculates the median of a given set of float64 values. The median is the middle value in a sorted list
// of numbers. If the list has an odd number of elements, it's the single middle element. If it has an even number of elements,
// it's the average of the two middle elements.
// Defines a function named 'median' that takes a slice of float64s and returns a single float64.
func median(values []float64) float64 {
	// Checks if the input slice of values is empty.
	if len(values) == 0 {
		// If the slice is empty, returns 0 (as there's no median for an empty set).
		return 0
	}

	// Create a copy and sort it
	// Creates a new slice of float64s with the same length as the input slice.
	sorted := make([]float64, len(values))
	// Copies all elements from the original 'values' slice into the new 'sorted' slice.
	copy(sorted, values)
	// Sorts the 'sorted' slice in ascending order.
	sort.Float64s(sorted)
	// Gets the number of elements in the sorted slice.
	n := len(sorted)
	// Checks if the number of elements 'n' is even.
	if n%2 == 0 {
		// If even, returns the average of the two middle elements.
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	// If odd, returns the single middle element.
	return sorted[n/2]
}

// The standardDeviation function calculates the sample standard deviation of a given set of numeric values. It takes a slice of
// float64 values and their pre-calculated mean as input. The sample standard deviation is a measure of the amount of variation
// or dispersion of a set of values, specifically when dealing with a sample of a larger population.
// Defines a function named 'standardDeviation' that takes a slice of float64s and a float64 mean, returning a float64.
func standardDeviation(values []float64, mean float64) float64 {
	// Checks if the input slice of values is empty.
	if len(values) == 0 {
		// If the slice is empty, returns 0 as the standard deviation
		return 0
	}
	// Initializes a variable 'variance' to 0.0 to accumulate squared differences.
	variance := 0.0
	// Iterates through each value 'v' in the 'values' slice.
	for _, v := range values {
		// Calculates the squared difference between the current value and the mean, and adds it to 'variance'.
		variance += math.Pow(v-mean, 2)
	}
	// Divides the sum of squared differences by (number of values - 1) to get the sample variance.
	variance /= float64(len(values) - 1) // Sample standard deviation
	// Returns the square root of the calculated variance, which is the standard deviation.
	return math.Sqrt(variance)
}

// Utility functions for min/max
// Defines a function named 'min' that accepts a variable number of float64 arguments and returns a single float64.
func min(values ...float64) float64 {
	// Checks if no values were provided to the function.
	if len(values) == 0 {
		// If the slice of values is empty, it returns 0
		return 0
	}
	// Initializes 'minVal' with the first value in the 'values' slice, assuming it's the minimum initially.
	minVal := values[0]
	// Iterates through the rest of the 'values' slice, starting from the second element.
	for _, v := range values[1:] {
		// Compares the current value 'v' with the current minimum 'minVal'.
		if v < minVal {
			// If 'v' is smaller, it updates 'minVal' to 'v'.
			minVal = v
		}
	}
	// Returns the smallest value found after checking all provided values.
	return minVal
}

// Defines a function named 'max' that accepts a variable number of float64 arguments and returns a single float64.
func max(values ...float64) float64 {
	// Checks if no values were provided to the function.
	if len(values) == 0 {
		// If the slice of values is empty, it returns 0
		return 0
	}
	// Initializes 'maxVal' with the first value in the 'values' slice, assuming it's the maximum initially.
	maxVal := values[0]
	// Iterates through the rest of the 'values' slice, starting from the second element.
	for _, v := range values[1:] {
		// Compares the current value 'v' with the current maximum 'maxVal'.
		if v > maxVal {
			// If 'v' is larger, it updates 'maxVal' to 'v'.
			maxVal = v
		}
	}
	// Returns the largest value found after checking all provided values.
	return maxVal
}

// The PrintReport method is part of the CSVAnalyzer struct and is responsible for formatting and displaying a comprehensive
// analysis report of the loaded CSV data. This report includes basic dataset information (rows, columns), a breakdown of each
// column's detected type (Text or Numeric), and detailed statistical analysis (sum, mean, median, standard deviation, min, max)
// for all columns identified as numeric.
// PrintReport formats and displays the analysis results
// Defines a method 'PrintReport' for CSVAnalyzer; it takes no arguments and returns nothing (only prints).
func (ca *CSVAnalyzer) PrintReport() {
	// Prints a title header for the report.
	fmt.Println("=== CSV Analysis Report ===")
	// Prints the total number of data rows and columns found in the dataset.
	fmt.Printf("Dataset: %d rows, %d columns\n\n", len(ca.dataset.Rows), len(ca.dataset.Headers))

	// Show column types
	// Prints a subheading for column type information.
	fmt.Println("Column Information")
	// Iterates through each header and its corresponding index in the dataset.
	for i, header := range ca.dataset.Headers {
		// Initializes the column type as "Text" by default.
		colType := "Text"
		// Checks if the current column (by index 'i') was identified as numeric in the 'NumericCols' map.
		if ca.dataset.NumericCols[i] {
			// If it's numeric, updates the 'colType' string to "Numeric".
			colType = "Numeric"
		}
		// Prints the column header and its determined type.
		fmt.Printf(" %s: %s\n", header, colType)
	}
	// Prints an empty line for better formatting.
	fmt.Println()

	// Show statistics for numeric columns
	// Calls the 'CalculateStats' method to get the statistical results for numeric columns.
	stats := ca.CalculateStats()
	// Checks if the returned 'stats' slice is empty (meaning no numeric columns were found or analyzed).
	if len(stats) == 0 {
		// Prints a message indicating no numeric columns for stats.
		fmt.Println("No Numeric Column Found for Statistical Analysis.")
		// Exits the function if no numeric columns were found.
		return
	}
	// Prints a subheading for the statistical analysis section.
	fmt.Println("Statistical Analysis (Numeric Columns):")
	// Prints a separator line for readability.
	fmt.Println("----------------------------------------")
	// Iterates through each 'ColumnStats' struct in the 'stats' slice.
	for _, stat := range stats {
		// Prints the name of the current column (from the 'ColumnStats' struct).
		fmt.Printf("\n%s:\n", stat.Name)
		// Prints the count of numeric values for the column.
		fmt.Printf("  Count:     %d\n", stat.Count)
		fmt.Printf("  Sum:       %.3f\n", stat.Sum)
		fmt.Printf("  Mean:      %.3f\n", stat.Mean)
		fmt.Printf("  Median:    %.3f\n", stat.Median)
		fmt.Printf("  Std Dev:   %.3f\n", stat.StdDev)
		fmt.Printf("  Min:       %.3f\n", stat.Min)
		fmt.Printf("  Max:       %.3f\n", stat.Max)
	}
}

// The createSampleData function serves as a utility to programmatically generate a CSV file with predefined sample sales data.
// This is typically used for testing or demonstration purposes, providing a consistent data source for the CSV analysis
// functionalities.
// createSampleData generates a test CSV file
// Defines a function named 'createSampleData' that takes a filename string and returns an error.
func createSampleData(filename string) error {
	// Attempts to create a new file with the given 'filename'. Returns a file object and an error.
	file, err := os.Create(filename)
	// Checks if an error occurred during file creation.
	if err != nil {
		// If an error, returns it immediately.
		return err
	}
	// Ensures the created file is closed when the function exits, regardless of success or failure.
	defer file.Close()

	// Sample data representing sales data
	// Declares a 2D slice of strings to hold the sample CSV data.
	data := [][]string{
		{"Product", "Price", "Quantity", "Revenue", "Category", "Rating"}, // First inner slice represents the CSV headers.
		{"Laptop", "999.99", "15", "14999.85", "Electronics", "4.5"},      // Subsequent inner slices represent data rows.
		{"Mouse", "25.50", "45", "1147.50", "Electronics", "4.2"},
		{"Keyboard", "75.00", "30", "2250.00", "Electronics", "4.7"},
		{"Monitor", "299.99", "12", "3599.88", "Electronics", "4.4"},
		{"Desk Chair", "199.50", "8", "1596.00", "Furniture", "4.1"},
		{"Notebook", "5.99", "100", "599.00", "Stationery", "4.0"},
		{"Pen Set", "12.99", "75", "974.25", "Stationery", "4.3"},
		{"Coffee Mug", "8.50", "60", "510.00", "Kitchen", "4.6"},
		{"Water Bottle", "15.99", "40", "639.60", "Kitchen", "4.4"},
		{"Backpack", "45.00", "25", "1125.00", "Accessories", "4.8"},
	}
	// Creates a new CSV writer that will write to the opened file.
	writer := csv.NewWriter(file)
	// Ensures all buffered data is written to the underlying file before the function returns.
	defer writer.Flush()
	// Iterates through each 'row' (which is a slice of strings) in the 'data' slice.
	for _, row := range data {
		// Writes the current 'row' to the CSV file. Checks for any writing errors.
		if err := writer.Write(row); err != nil {
			// If an error occurs during writing, returns the error.
			return err
		}
	}
	// If the file is created and all data is written successfully, returns nil (no error).
	return nil
}

func main() {
	// Check command line arguments
	// Checks if the number of command-line arguments is less than 2 (program name + at least one argument).
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run . <csv-file>")
		fmt.Println("Or: go run . sample  (to create and analyze sample data)")
		os.Exit(1)
	}

	// Retrieves the first command-line argument (which should be the filename or "sample").
	filename := os.Args[1]

	// If user wants sample data, create it
	// Checks if the provided argument is "sample".
	if filename == "sample" {
		// If "sample", sets the filename to a default "sample_data.csv".
		filename = "sample_data.csv"
		// Informs the user that sample data is being created.
		fmt.Println("Creating sample data file:", filename)
		// Calls the 'createSampleData' function to generate the CSV file.
		if err := createSampleData(filename); err != nil {
			// If an error occurs during sample data creation, logs the error and exits.
			log.Fatal("Error creating sample data:", err)
		}
		// Confirms successful creation of sample data.
		fmt.Println("Sample data created successfully!")
		// Prints an empty line for formatting.
		fmt.Println()
	}

	// Create analyzer and process the file
	// Creates a new instance of CSVAnalyzer using the constructor function.
	analyzer := NewCSVAnalyzer()
	// Informs the user which CSV file is being loaded.
	fmt.Printf("Loading CSV file: %s\n", filename)
	// Calls the 'LoadCSV' method on the analyzer to load and parse the CSV file.
	if err := analyzer.LoadCSV(filename); err != nil {
		// If an error occurs during CSV loading, logs the error and exits.
		log.Fatal("Error loading CSV:", err)
	}

	// Calls the 'PrintReport' method on the analyzer to display the analysis results.
	analyzer.PrintReport()
}
