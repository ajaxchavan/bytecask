package core

import (
	"strings"
)

type Cmd struct {
	Cmd  string
	Args []string
}

func evalValue(line string) string {
	line = strings.TrimSpace(line)
	var size = len(line)
	if size == 0 {
		return ""
	}

	var index = 0
	var char uint8
	switch line[0] {
	case '\'':
		char = '\''
		if line[size-1] != char {
			return ""
		}
		index = 1
	case '"':
		char = '"'
		if line[size-1] != char {
			return ""
		}
		index = 1
	default:
		char = ' '
	}
	for ; index < size; index++ {
		if line[index] == char {
			break
		}
	}

	switch char {
	case ' ':
		if index == size {
			return line
		}
		return ""
	default:
		if index != size-1 {
			return ""
		}
		return line[1 : size-1]
	}
}

func evalCmd(line string) (int, string) {
	size := len(line)
	if size == 0 {
		return 0, ""
	}

	var cmd strings.Builder
	var end = 0
	var start = 0
	var char uint8

	switch line[0] {
	case '\'':
		char = '\''
		start = 1
	case '"':
		char = '"'
		start = 1
	default:
		char = ' '
	}

	for end = start; end < size; end++ {
		if line[end] == char {
			end++
			break
		}
		cmd.WriteByte(line[end])
	}

	switch char {
	case ' ':
		if end == size {
			return size, line
		}
		return end, line[start : end-1]
	default:
		if line[end-1] != char {
			return 0, ""
		}
		return end, line[start : end-1]
	}
}

func NewCmd(line string) *Cmd {
	line = strings.TrimSpace(line)
	j, cmd := evalCmd(line)
	if cmd == "" {
		return nil
	}
	cmd = strings.ToUpper(cmd)
	line = line[j:]
	line = strings.TrimSpace(line)

	k, key := evalCmd(line)
	if key == "" && cmd != pingCmd {
		return nil
	}
	line = line[k:]
	line = strings.TrimSpace(line)

	if cmd != setCmd {
		if len(line) != 0 {
			return nil
		}

		return &Cmd{
			Cmd:  cmd,
			Args: []string{key},
		}
	}

	value := evalValue(line)
	if value == "" {
		return nil
	}

	return &Cmd{
		Cmd:  cmd,
		Args: []string{key, value},
	}
}
