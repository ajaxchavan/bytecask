package core

var (
	RESP_OK  = []byte("OK\r\n")
	RESP_ONE = []byte("1\r\n")
)

func (s *Store) evalPing(args []string) []byte {
	if len(args) > 1 {
		return Encode("ERR wrong number of arguments for 'ping' command")
	}
	// msg := len(args) == 1 ? args[0] : "PONG"
	if len(args) == 1 {
		return Encode(args[0])
	}
	return Encode("PONG")
}

func (s *Store) evalGet(args []string) []byte {
	if len(args) > 1 || len(args) == 0 {
		return Encode("ERR wrong number of arguments for 'get' command")
	}
	value, err := s.Get(args[0])
	if err != nil {
		return Encode(err.Error())
	}

	return Encode(value)
}

func (s *Store) evalSet(args []string) []byte {
	if len(args) > 2 || len(args) < 2 {
		return Encode("ERR wrong number of arguments for 'get' command")
	}
	if err := s.Set(args[0], []byte(args[1])); err != nil {
		return Encode(err.Error())
	}
	return RESP_OK
}

func (s *Store) evalDelete(args []string) []byte {
	if len(args) > 1 || len(args) < 1 {
		return Encode("ERR wrong number of arguments for 'get' command")
	}
	if err := s.Del(args[0]); err != nil {
		return Encode(err.Error())
	}
	return RESP_ONE
}

func (s *Store) executeCmd(cmd *CrowCmd) []byte {
	switch cmd.Cmd {
	case "PING":
		return s.evalPing(cmd.Args)
	case "GET":
		return s.evalGet(cmd.Args)
	case "SET":
		return s.evalSet(cmd.Args)
	case "DEL":
		return s.evalDelete(cmd.Args)
	default:
		return s.evalPing(cmd.Args)
	}
}

func (s *Store) EvalAndResponse(cmd *CrowCmd, client *Client) {
	_, _ = client.Write(s.executeCmd(cmd))
}
