package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
)

// defining person structure
type Person struct {
	firstname  string `json: "first_name"`
	lastname   string `json: "last_name"`
	age        int64  `json: "age"`
	bloodgroup string `json: "blood_group"`
}

func dataRetrievalandStorage() {
	// opening and reading csv file with data
	csvFile, _ := os.Open("data.csv")
	reader := csv.NewReader(csvFile)

	// database connection
	fmt.Println("Connecting to Bloodbank Database")
	db := conn()

	//maintaining open database connection until end of function
	defer db.Close()

	for {
		// traversing every line in data.csv
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}

		// extracting relevant information
		firstname := line[0]
		lastname := line[1]
		age, _ := strconv.ParseInt(line[2], 0, 8)
		bloodgroup := line[3]

		// record insertion
		insert(db, firstname, lastname, age, bloodgroup)
	}

	fmt.Println("Data Insertion Successful\n")
}

func conn() (db *sql.DB) {
	// opening database connectio with bloodbank database on localhost
	db, error := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/bloodbank")

	if error != nil {
		panic(error.Error())
	} else {
		fmt.Println("Successfully Connected to Database\n\n")
		return db
	}

}

func insert(db *sql.DB, first string, last string, age int64, blood string) {
	fmt.Printf("Inserting %s's record into persons table.\n", first)
	// preparing
	ins, err := db.Prepare("INSERT INTO persons(first_name, last_name, age, blood_group) VALUES(?,?,?,?)")
	if err != nil {
		panic(err.Error())
	}

	// executing insert with extracted information
	ins.Exec(first, last, age, blood)

	if err != nil {
		panic(err.Error())
	}

}

func main() {

	dataRetrievalandStorage()
}
