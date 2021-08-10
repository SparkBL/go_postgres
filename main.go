package main

import (
	"context"
	"encoding/csv"
	"go_postgres/config"
	"go_postgres/db"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/juliangruber/go-intersect"
	log "github.com/sirupsen/logrus"
)

type Group struct {
	GroupId  int
	RegionId int
	Fraction float64
	Members  []int
}

func ThreadWork(region_id int, user_array []int, c chan Group, outputDir string) {
	csvFile, err := os.Create(outputDir + "/" + strconv.Itoa(region_id) + ".csv")
	if err != nil {
		log.WithError(err).Fatalf("failed creating file: %s", err)
	}
	csvwriter := csv.NewWriter(csvFile)
	csvwriter.Comma = ';'
	csvwriter.Write([]string{"group_id", "region_id", "fraction"})
	log.WithTime(time.Now()).Println("Starting region ", region_id)
	//var elapsed []int64
	for group := range c {
		//start := time.Now()
		intersection := intersect.Hash(user_array, group.Members).([]interface{})
		//elapsed = append(elapsed, time.Since(start).Milliseconds())
		group.Fraction = float64(len(intersection)) / float64(len(group.Members))
		if group.Fraction > 0.000000 {
			group.RegionId = region_id
			csvwriter.Write([]string{strconv.Itoa(group.GroupId), strconv.Itoa(group.RegionId), strconv.FormatFloat(group.Fraction, 'f', 8, 64)})
		}

	}
	csvwriter.Flush()
	csvFile.Close()
	/*total := int64(0)
	for _, number := range elapsed {
		total = total + number
	}
	average := total / int64(len(elapsed))*/
	log.WithTime(time.Now()).Println("Completed region", region_id) //, "in average", average, "millisecs")
}
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func main() {
	log.Println("PID:", os.Getpid())
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	conf, err := config.LoadConfig()
	if err != nil {
		log.Panic(err)
	}
	if ok, _ := exists(conf.Outputdir); !ok {
		os.Mkdir(conf.Outputdir, 0755)
	}

	DBClient := db.NewDbClient(conf.DBConnectionString)
	users, err := DBClient.StoreUserIds(conf.UsersPath)
	if err != nil {
		log.WithTime(time.Now()).WithError(err).Fatalln("Couldn't load users. Aborting..")
	}

	conn, err := DBClient.NewConnection()
	if err != nil {
		log.WithTime(time.Now()).WithError(err).Fatalln("Failed to connect to PostgreSQL server")

	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), "select group_id,items from "+conf.MembersPath)
	if err != nil {
		log.WithTime(time.Now()).WithError(err).Fatalln("Failed to query members.")
	}

	var chans []chan Group
	for region_id, user_array := range *users {
		chans = append(chans, make(chan Group, conf.ChannelBuffer))
		go ThreadWork(region_id, user_array, chans[len(chans)-1], conf.Outputdir)
	}
	for rows.Next() {
		var group_id int
		var members []int
		rows.Scan(&group_id, &members)
		for i := 0; i < len(chans); i++ {
			chans[i] <- Group{GroupId: group_id, Members: members}
		}
	}
	rows.Close()
	for _, c := range chans {
		close(c)
	}
	<-stop
}
