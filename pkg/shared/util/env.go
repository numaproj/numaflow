package util

import (
	"os"
)

func LookupEnvStringOr(key, defaultValue string) string {
	if v, existing := os.LookupEnv(key); existing && v != "" {
		return v
	} else {
		return defaultValue
	}
}
