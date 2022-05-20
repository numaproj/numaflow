package fixtures

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
)

func InvokeE2EAPI(format string, args ...interface{}) string {
	url := "http://127.0.0.1:8378" + fmt.Sprintf(format, args...)
	log.Printf("GET %s\n", url)
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	log.Printf("> %s\n", resp.Status)
	body := ""
	defer resp.Body.Close()
	for s := bufio.NewScanner(resp.Body); s.Scan(); {
		x := s.Text()
		if strings.Contains(x, "ERROR") { // hacky way to return an error from an octet-stream
			panic(errors.New(x))
		}
		log.Printf("> %s\n", x)
		body += x
	}
	if resp.StatusCode >= 300 {
		panic(errors.New(resp.Status))
	}
	return body
}

func InvokeE2EAPIPOST(format string, body string, args ...interface{}) string {
	url := "http://127.0.0.1:8378" + fmt.Sprintf(format, args...)
	log.Printf("Post %s\n", url)
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	log.Printf("> %s\n", resp.Status)
	defer resp.Body.Close()
	for s := bufio.NewScanner(resp.Body); s.Scan(); {
		x := s.Text()
		if strings.Contains(x, "ERROR") { // hacky way to return an error from an octet-stream
			panic(errors.New(x))
		}
		log.Printf("> %s\n", x)
		body += x
	}
	if resp.StatusCode >= 300 {
		panic(errors.New(resp.Status))
	}
	return body
}
