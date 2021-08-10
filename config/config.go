package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	MembersPath        string `json:"members_path"`
	UsersPath          string `json:"users_path"`
	InsertPath         string `json:"insert_path"`
	DBConnectionString string `json:"db_conn_str"`
	ChannelBuffer      int    `json:"channel_buffer"`
	Outputdir          string `json:"output_dir"`
}

func LoadConfig() (Config, error) {
	configFile, _ := os.Open("config.json")
	defer configFile.Close()
	decoder := json.NewDecoder(configFile)
	config := Config{}
	err := decoder.Decode(&config)
	if err != nil {
		err = fmt.Errorf("Error while parsing configuration file: '%s'", err)
	}
	return config, err
}
