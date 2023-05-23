package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"sync"
)

func main() {
	f, err := os.Open("test.txt")
	if err != nil {
		fmt.Println("cannot able to read the file", err)
		return
	}
	defer f.Close()

	linesPool := sync.Pool{New: func() interface{} {
		lines := make([]byte, 500*1024)
		return lines
	}}
	stringPool := sync.Pool{New: func() interface{} {
		lines := ""
		return lines
	}}
	slicePool := sync.Pool{New: func() interface{} {
		lines := make([]string, 100)
		return lines
	}}

	r := bufio.NewReader(f)

	var wg sync.WaitGroup

	for {
		// linePools.Get() 을 통해 재사용 가능한 []byte 를 가지고 있는 객체 생성
		buf := linesPool.Get().([]byte)
		// 읽어서 버퍼에 담음
		n, err := r.Read(buf) // loading chunk into buffer
		// 버퍼의 길이가 남으면 잘라서 버림
		buf = buf[:n]

		if n == 0 {
			if err != nil {
				fmt.Println(error.Error(err))
				break
			}
			if err == io.EOF {
				break
			}
			//return err
		}

		nextUtilNewLine, err := r.ReadBytes('\n') // read entire line

		if err != io.EOF {
			buf = append(buf, nextUtilNewLine...)
		}

		wg.Add(1)
		go func() {
			ProcessChunk(buf, &linesPool, &stringPool, &slicePool)
			wg.Done()
		}()
	}
	wg.Wait()
}

func ProcessChunk(chunk []byte, linesPool *sync.Pool, stringPool *sync.Pool, slicePool *sync.Pool) {
	var wg2 sync.WaitGroup
	// stringPool.Get() 을 통해 재사용 가능한 string 을 가지고 있는 pool 을 생성
	documents := stringPool.Get().(string)
	// 재사용 가능한 string 풀에 chunk 를 string 화 하여 담아준다.
	documents = string(chunk)
	// linePool 에 chunk 를 다시 반환한다.
	linesPool.Put(chunk)

	// \n 을 기준으로 string 을 잘라 slice 에 담아준다.
	documentsSlice := slicePool.Get().([]string)
	documentsSlice = strings.Split(documents, "\n")

	// 사용을 마친, string 을 가지고 있는 재사용 가능한 객체를 반납한다.
	// documents 를 잘라 documentSlice 에 담아주었으니 더 이상 사용할 필요가 없다.
	stringPool.Put(documents)

	chunkSize := 100 // thread 별로 100 개의 document 묶음을 처리할 것이다.
	n := len(documentsSlice)

	// documentSlice 에 담긴 line 의 개수를 chunkSize(100) 으로 나눈다.
	numberOfThread := n / chunkSize
	// 아래 코드는 thread 당 100 개의 job 을 처리하게 하고 나머지 또한 처리할 수 있게 하기 위함이다.
	// 만약 n(130) / chunkSize(100) 의 경우 numberOfThread 는 1 이다.
	// 이 경우 n % chunkSize 는 30 이 될 것이다. 따라서 100 이하의 것들을 처리하기 위한 thread 를 하나 더 만들어주는 것이다.
	if n%chunkSize != 0 {
		numberOfThread++
	}

	length := len(documentsSlice)

	// 각 100 개의 chunk 를 handling 한다.
	for i := 0; i < length; i += chunkSize {
		wg2.Add(1)

		go func(s int, e int) {
			for i := s; i < e; i++ {
				text := documentsSlice[i]
				if len(text) == 0 {
					continue
				}
				testHandler(text)
			}

			wg2.Done()
		}(i*chunkSize, int(math.Min(float64((i+1)*chunkSize), float64(len(documentsSlice)))))
	}

	wg2.Wait()
	documentsSlice = nil
}

func testHandler(text string) {
	fmt.Println(text)
}
