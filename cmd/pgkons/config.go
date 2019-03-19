package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/manifoldco/promptui"
)

type CFG struct {
	Name     string `json:"config_name"`
	DBName   string `json:"dbName"`
	Password string `json:"password"`
	Port     string `json:"port"`
	SSLMode  string `json:"sslMode"`
	Username string `json:"username"`
}

func (c CFG) DBConnection() string {
	var parts []string
	if c.Username != "" {
		parts = append(parts, fmt.Sprintf("user='%s'", c.Username))
	}
	if c.Password != "" {
		parts = append(parts, fmt.Sprintf("password='%s'", c.Password))
	}
	if c.DBName != "" {
		parts = append(parts, fmt.Sprintf("dbname='%s'", c.DBName))
	}
	if c.Port != "" {
		parts = append(parts, fmt.Sprintf("port='%s'", c.Port))
	}
	if c.SSLMode != "" {
		parts = append(parts, "sslmode="+c.SSLMode)
	}
	return strings.Join(parts, " ")
}

func newDBCFG() (string, error) {
	cfg, err := func() (CFG, error) {
		cfgs, err := configFile()
		if err == nil {
			return selectConfig(cfgs)
		}
		return CFG{}, err
	}()
	if err == nil {
		return cfg.DBConnection(), nil
	}

	var newCFG CFG
	prompts := []struct {
		prompt promptui.Prompt
		fn     func(string)
	}{
		{
			prompt: promptui.Prompt{
				Label:     "Username",
				AllowEdit: true,
				Validate:  validateEmptyInput("username"),
				Default:   "postgres",
			},
			fn: func(v string) { newCFG.Username = v },
		},
		{
			prompt: promptui.Prompt{
				Label:     "Password",
				AllowEdit: true,
				Validate:  validateEmptyInput("password"),
				Default:   "postgres",
				Mask:      '●',
			},
			fn: func(v string) { newCFG.Password = v },
		},
		{
			prompt: promptui.Prompt{
				Label:     "Database",
				AllowEdit: true,
				Validate:  validateEmptyInput("database"),
				Default:   "postgres",
			},
			fn: func(v string) { newCFG.DBName = v },
		},
		{
			prompt: promptui.Prompt{
				Label:     "Port",
				AllowEdit: true,
				Validate:  validateEmptyInput("port"),
				Default:   "5433",
			},
			fn: func(v string) { newCFG.Port = v },
		},
	}

	for _, p := range prompts {
		entry, err := p.prompt.Run()
		if err != nil {
			return "", err
		}
		overwritePrevLine()
		p.fn(entry)
	}

	sslModes := []string{"disable", "require", "verify-ca", "verify-full"}
	newCFG.SSLMode, err = selectStr("SSL Mode", sslModes)
	if err != nil {
		return "", err
	}
	overwritePrevLine()

	confirm, err := (&promptui.Prompt{
		Label:     "Save configuration",
		IsConfirm: true,
	}).Run()
	overwritePrevLine()
	if err != nil || confirm != "y" {
		return newCFG.DBConnection(), nil
	}

	newCFG.Name, err = (&promptui.Prompt{
		Label:    "Config Name",
		Validate: validateEmptyInput("filename"),
	}).Run()
	overwritePrevLine()
	if err == nil {
		if err := saveNewConfig(newCFG); err != nil {
			fmt.Println(err)
		}
	}
	return newCFG.DBConnection(), nil
}

func configFile() ([]CFG, error) {
	konsdir := os.Getenv("HOME") + "/.pgkons"
	_, err := os.Lstat(konsdir)
	if os.IsNotExist(err) {
		if err := os.Mkdir(konsdir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	file, err := func() (*os.File, error) {
		configFilePath := konsdir + "/config.json"
		_, err := os.Stat(configFilePath)
		if err == nil {
			return os.Open(configFilePath)
		}
		f, err := os.Create(configFilePath)
		if err != nil {
			return nil, err
		}
		if err := json.NewEncoder(f).Encode([]CFG{}); err != nil {
			return nil, err
		}
		return f, nil
	}()
	if err != nil {
		return nil, err
	}
	defer func() {
		check(file.Close())
	}()

	var cfgs []CFG
	if err := json.NewDecoder(file).Decode(&cfgs); err != nil {
		return nil, nil
	}
	return cfgs, nil
}

func saveNewConfig(newCFG CFG) error {
	konsdir := os.Getenv("HOME") + "/.pgkons"
	_, err := os.Lstat(konsdir)
	if os.IsNotExist(err) {
		if err := os.Mkdir(konsdir, os.ModePerm); err != nil {
			return err
		}
	}

	configFilePath := konsdir + "/config.json"
	file, err := func() (*os.File, error) {
		_, err := os.Stat(configFilePath)
		if err == nil {
			return os.Open(configFilePath)
		}
		return os.Create(configFilePath)
	}()
	if err != nil {
		return err
	}
	defer file.Close()

	var cfgs []CFG
	if err := json.NewDecoder(file).Decode(&cfgs); err != nil {
		fmt.Println(err)
	}
	check(file.Close())

	newF, err := os.Create(configFilePath)
	if err != nil {
		return err
	}

	cfgs = append(cfgs, newCFG)
	b, err := json.MarshalIndent(cfgs, "", "\t")
	if err != nil {
		return err
	}
	_, err = newF.Write(b)
	return err
}

func selectConfig(cfgs []CFG) (CFG, error) {
	if len(cfgs) == 0 {
		return CFG{}, errors.New("no configs provided")
	}

	confirm, err := (&promptui.Prompt{
		Label:     "Use previous config",
		IsConfirm: true,
	}).Run()
	overwritePrevLine()
	if err != nil && confirm != "y" {
		return CFG{}, errors.New("config not used")
	}

	searcher := func(input string, index int) bool {
		cfg := cfgs[index]
		name := strings.Replace(strings.ToLower(cfg.Name), " ", "", -1)
		input = strings.Replace(strings.ToLower(input), " ", "", -1)
		return strings.Contains(name, input)
	}

	templates := &promptui.SelectTemplates{
		Label:    "{{ . }}?",
		Active:   "» {{ .Name | cyan }} : {{ .Username | green }}  : {{ .DBName | green }} : {{ .SSLMode | green }}",
		Inactive: "  {{ .Name | cyan }} : {{ .Username | green }}  : {{ .DBName | green }} : {{ .SSLMode | green }}",
		Selected: "{{ print .Name }}",
		Details: `
 ------------ Config ------------
 {{ "Name:" | faint }}	{{ .Name }}
 {{ "Username:" | faint }}	{{ .Username }}
 {{ "Database Name:" | faint }}	{{ .DBName }}
 {{ "SSL Mode:" | faint }}	{{ .SSLMode }}`,
	}

	sel := promptui.Select{
		HideHelp:          true,
		Label:             "Configs",
		Items:             cfgs,
		Searcher:          searcher,
		Size:              selectSize(templates),
		StartInSearchMode: true,
		Templates:         templates,
	}

	i, _, err := sel.Run()
	if err != nil {
		return CFG{}, err
	}
	overwritePrevLine()

	return cfgs[i], nil
}
