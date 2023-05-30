package main

import (
	"api-public-data/big_file_handler/dataHandler"
	"api-public-data/big_file_handler/dbHandler"
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

func main() {
	pool := &sync.Pool{
		New: func() interface{} {
			return make([][]string, 0)
		},
	}

	start := time.Now()

	// mongoDB 연결
	host := "mongodb://localhost:27017"
	db := "mydb"
	model := "buildings"

	err := dbHandler.ConnectMongoDB(host, db, model)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer dbHandler.DisConnectMongoDB()

	// 파일처리
	// 입력 파일 경로
	inputFilePath := "/Users/sunghyun/Desktop/mart_djy_03.txt"

	limit := 100

	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)

	var lines []string
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 200) // Goroutine 최대 개수

	for scanner.Scan() {
		// line 단위로 byte 를 읽어온다.
		byteLine := scanner.Bytes()

		// byte 를 인코딩 후 string 으로 변경
		str := dataHandler.EncodeBytes(byteLine)

		lines = append(lines, str)

		// lines 개수가 limit에 도달하면 삽입 작업을 수행하고 lines를 초기화
		if len(lines) == limit {
			linesCopy := make([]string, len(lines))
			copy(linesCopy, lines)
			// Goroutine 실행
			wg.Add(1)
			semaphore <- struct{}{} // Semaphore 를 획득하기 위해 빈 구조체 값을 보냄
			go func(data []string) {
				defer func() {
					<-semaphore // Semaphore 를 해제하여 다른 고루틴에게 실행을 양보함
					wg.Done()
				}()

				documentPool := pool.Get().([][]string)

				for _, item := range data {
					documentPool = append(documentPool, dataHandler.SplitByPipeline(item))
				}

				documents := dataHandler.ToMongoInsertType(documentPool)
				err := dbHandler.InsertDocuments(documents)
				if err != nil {
					fmt.Println(err)
				}
				documentPool = documentPool[:0]
				pool.Put(documentPool)
			}(linesCopy)

			lines = lines[:0]
		}
	}

	// 파일의 끝에 도달하면 남은 데이터를 삽입하고 종료
	if len(lines) > 0 {
		wg.Add(1)
		semaphore <- struct{}{} // Semaphore 를 획득하기 위해 빈 구조체 값을 보냄
		go func(data []string) {
			defer func() {
				<-semaphore // Semaphore 를 해제하여 다른 고루틴에게 실행을 양보함
				wg.Done()
			}()

			documentPool := pool.Get().([][]string)

			for _, item := range data {
				documentPool = append(documentPool, dataHandler.SplitByPipeline(item))
			}

			documents := dataHandler.ToMongoInsertType(documentPool)
			err := dbHandler.InsertDocuments(documents)
			if err != nil {
				fmt.Println(err)
			}
			documentPool = documentPool[:0]
			pool.Put(documentPool)
		}(lines)
	}

	wg.Wait() // 모든 Goroutine이 종료될 때까지 대기

	// 실행 시간 측정 종료
	duration := time.Since(start)

	// 실행 시간 출력
	fmt.Println("실행 시간:", duration)
}
