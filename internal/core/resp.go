package core

import "fmt"

func Encode(value interface{}) []byte {
	return []byte(fmt.Sprintf("%s\r\n", value))
}
