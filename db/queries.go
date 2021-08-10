package db

import (
	"context"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
)

func (o *dbClient) StoreUserIds(readPath string) (*map[int][]int, error) {
	conn, err := o.NewConnection()
	if err != nil {
		log.WithError(err).Debug("Failed to connect to PostgreSQL server")
		return nil, err
	}
	defer conn.Close(context.Background())
	users := make(map[int][]int)
	rows, err := conn.Query(context.TODO(), "select region_id,users from "+readPath)
	if err != nil {
		log.WithError(err).Debug("Failed to execute query in PostgreSQL server")
		return nil, err
	}
	for rows.Next() {
		var region_id int
		var user_array []int
		rows.Scan(&region_id, &user_array)
		sort.Ints(user_array)
		users[region_id] = user_array
	}
	rows.Close()
	log.WithTime(time.Now()).Println("Users were sorted and stored, number of rows: ", len(users))
	return &users, nil
}
