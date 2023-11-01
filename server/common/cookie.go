package common

import (
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	maxCookieLength = 4096
	maxValueLength  = maxCookieLength - 500
	// max number of chunks a cookie can be broken into
	maxCookieNumber = 10
)

type IdentityCookie struct {
	Key   string
	Value string
}

// MakeCookieMetadata generates a string representing a Web cookie.  Yum!
func MakeCookieMetadata(key, value string) ([]IdentityCookie, error) {
	numberOfCookies := int(math.Ceil(float64(len(value)) / float64(maxValueLength)))
	if numberOfCookies > maxCookieNumber {
		return nil, fmt.Errorf("the authentication token is %d characters long and requires %d cookies but the max number of cookies is %d", len(value), numberOfCookies, maxCookieNumber)
	}
	return splitCookie(key, value), nil
}

// splitCookie splits a cookie because browser requires cookie to be < 4kb.
// In order to support cookies longer than 4kb, we split cookie into multiple chunks.
func splitCookie(key, value string) []IdentityCookie {
	var cookies []IdentityCookie
	valueLength := len(value)
	numberOfChunks := int(math.Ceil(float64(valueLength) / float64(maxValueLength)))

	var end int
	for i, j := 0, 0; i < valueLength; i, j = i+maxValueLength, j+1 {
		end = i + maxValueLength
		if end > valueLength {
			end = valueLength
		}
		var cookie IdentityCookie
		if j == 0 {
			cookie = IdentityCookie{
				Key:   key,
				Value: fmt.Sprintf("%d:%s", numberOfChunks, value[i:end]),
			}
		} else {
			cookie = IdentityCookie{
				Key:   fmt.Sprintf("%s-%d", key, j),
				Value: value[i:end],
			}
		}
		cookies = append(cookies, cookie)
	}
	return cookies
}

// JoinCookies combines chunks of cookie based on Key as prefix. It returns cookie
// Value as string. cookieString is of format key1=value1; key2=value2; key3=value3
// first chunk will be of format numaflow.token=<numberOfChunks>:token; attributes
func JoinCookies(key string, cookieList []*http.Cookie) (string, error) {
	cookies := make(map[string]string)
	for _, cookie := range cookieList {
		if !strings.HasPrefix(cookie.Name, key) {
			continue
		}
		val, _ := url.QueryUnescape(cookie.Value)
		cookies[cookie.Name] = val
	}

	var sb strings.Builder
	var numOfChunks int
	var err error
	var token string
	var ok bool

	if token, ok = cookies[key]; !ok {
		return "", fmt.Errorf("failed to retrieve cookie %s", key)
	}
	parts := strings.Split(token, ":")

	if len(parts) >= 2 {
		if numOfChunks, err = strconv.Atoi(parts[0]); err != nil {
			return "", err
		}
		sb.WriteString(strings.Join(parts[1:], ":"))
	} else {
		return "", fmt.Errorf("invalid cookie for key %s", key)
	}

	for i := 1; i < numOfChunks; i++ {
		sb.WriteString(cookies[fmt.Sprintf("%s-%d", key, i)])
	}
	return sb.String(), nil
}
