package core

const (
	pingCmd = "PING"
	setCmd  = "SET"
	delCmd  = "DEL"
	getCmd  = "GET"
)

func (s *Store) evalPing(key string) []byte {
	msg := func() string {
		if key == "" {
			return "PONG"
		}
		return key
	}()
	return Encode(msg)
}

func (s *Store) evalGet(key string) []byte {
	return Encode(s.get(key))
}

func (s *Store) evalSet(key, value string) []byte {
	return Encode(s.set(key, []byte(value)))
}

func (s *Store) evalDelete(key string) []byte {
	return Encode(s.del(key))
}

func (s *Store) executeCmd(cmd *Cmd) []byte {
	switch cmd.Cmd {
	case pingCmd:
		return s.evalPing(cmd.Args[0])
	case getCmd:
		return s.evalGet(cmd.Args[0])
	case setCmd:
		return s.evalSet(cmd.Args[0], cmd.Args[1])
	case delCmd:
		return s.evalDelete(cmd.Args[0])
	default:
		return s.evalPing(cmd.Args[0])
	}
}

func (s *Store) EvalAndResponse(cmd *Cmd, client *Client) {
	_, _ = client.Write(s.executeCmd(cmd))
}
