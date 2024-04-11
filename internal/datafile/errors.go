package datafile

const (
	ErrEmptyData Error = "empty data"
)

type Error string

func (e Error) Error() string {
	return string(e)
}

func (e Error) Is(target error) bool {
	t, ok := target.(Error)
	return ok && string(e) == string(t)
}
