package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"go_postgres/config"
	"go_postgres/db"
	"go_postgres/utils"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/juliangruber/go-intersect"
	log "github.com/sirupsen/logrus"
)

func ThreadWork(region_id int, user_array []int, conf config.Config) {
	DBClient := db.NewDbClient(conf.DBConnectionString)
	conn, err := DBClient.NewConnection()
	if err != nil {
		log.WithTime(time.Now()).WithError(err).Fatalln("Failed to connect to PostgreSQL server")

	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), "select group_id,items from "+conf.MembersPath)
	if err != nil {
		log.WithTime(time.Now()).WithError(err).Fatalln("Failed to query members.")
	}

	csvFile, err := os.Create(conf.Outputdir + "/" + strconv.Itoa(region_id) + ".csv")
	if err != nil {
		log.WithError(err).Fatalf("failed creating file: %s", err)
	}
	csvwriter := csv.NewWriter(csvFile)
	csvwriter.Comma = ';'
	csvwriter.Write([]string{"group_id", "region_id", "fraction", "region_member_count"})
	log.WithTime(time.Now()).Println("Starting region ", region_id)
	for rows.Next() {
		var group_id int
		var members []int
		rows.Scan(&group_id, &members)
		intersection := intersect.Hash(user_array, members).([]interface{})
		fraction := float64(len(intersection)) / float64(len(members))
		if fraction > conf.Precision {
			csvwriter.Write([]string{strconv.Itoa(group_id), strconv.Itoa(region_id), strconv.FormatFloat(fraction, 'f', 8, 64), strconv.Itoa(len(intersection))})
		}
	}
	rows.Close()
	csvwriter.Flush()
	csvFile.Close()
	log.WithTime(time.Now()).Println("Completed region", region_id) //, "in average", average, "millisecs")
}

func main() {
	utils.InitLogger()

	f, err := os.OpenFile("output.log", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Printf("error opening file: %v", err)
	}

	// don't forget to close it
	defer f.Close()
	// Output to stderr instead of stdout, could also be a file.
	log.SetOutput(f)

	log.Println("PID:", os.Getpid())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	conf, err := config.LoadConfig()
	if err != nil {
		log.Panic(err)
	}
	if ok, _ := utils.Exists(conf.Outputdir); !ok {
		os.Mkdir(conf.Outputdir, 0755)
	}
	DBClient := db.NewDbClient(conf.DBConnectionString)
	users, err := DBClient.StoreUserIds(conf.UsersPath)
	if err != nil {
		log.WithTime(time.Now()).WithError(err).Fatalln("Couldn't load users. Aborting..")
	}
	for region_id, user_array := range *users {
		go ThreadWork(region_id, user_array, conf)
	}

	<-stop
}
