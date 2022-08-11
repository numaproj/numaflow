package pbq

type pbq struct {
	Store *PBQStore
	out   chan [][]byte
}

func (q *pbq) Read(p []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (q *pbq) Clean() {
	//TODO implement me
	panic("implement me")
}

func (q *pbq) Write(p []byte) (n int, err error) {
	//TODO implement me
	panic("implement me")
}

func (q *pbq) Close() error {
	//TODO implement me
	panic("implement me")
}
