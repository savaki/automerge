package encoding

func MustInt64(got []int64, err error) []int64 {
	if err != nil {
		panic(err)
	}
	return got
}
