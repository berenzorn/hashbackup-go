package file

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

type ShaFile struct {
	Path  string
	Array map[string]string
}

func (s *ShaFile) ReadFile() (shaFile *os.File, err error) {

	var rd *bufio.Reader
	path := fmt.Sprintf(s.Path + string(os.PathSeparator) + ".sha1")
	shaFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}

	rd = bufio.NewReader(shaFile)
	for {
		line, err := rd.ReadString('\n')
		line = strings.TrimRight(line, "\r\n")
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		hash := strings.Split(line, "   ")
		s.Array[hash[1]] = hash[0]
	}
	return
}

func (s *ShaFile) ReadSha1(name string) (sha string) {
	return s.Array[name]
}
