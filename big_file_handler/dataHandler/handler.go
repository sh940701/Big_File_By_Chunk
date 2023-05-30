package dataHandler

import (
	"fmt"
	"golang.org/x/text/encoding/korean"
	"strconv"
	"strings"
)

func TestHandler(text string) {
	textSlice := strings.Split(text, "|")
	if len(textSlice[5]) == 0 {
		fmt.Println(text)
	}
}

type Building struct {
	ID             string  `bson:"_id"`              // 건물 PK
	Address        string  `bson:"address"`          // 지번주소
	Birthday       string  `bson:"birthday"`         // 준공년도
	GrossFloorArea float64 `bson:"gross_floor_area"` // 연면적
	Underground    int     `bson:"underground"`      // 지하층
	Ground         int     `bson:"ground"`           // 지상층
	Passenger      int     `bson:"passenger"`        // 승객용 엘리베이터 수
	Emergency      int     `bson:"emergency"`        // 비상용 엘리베이터 수
	Slots          int     `bson:"slots"`            // 총 주차 대수
	Comment        string  `bson:"comment"`          // 주차 형태
	Street         string  `bson:"street"`           // 도로명 주소
	Name           string  `bson:"name"`             // 빌딩 이름
}

// ReadLines 주어진 파일에 대해 line 별로 읽어 이를 반환하는 함수
func ReadLines(inputFilePath string, limit int) (lines [][]string) {

	return lines
}

// EncodeBytes 주어진 문자열을 EUC-KR 로 디코딩 해 주는 함수
func EncodeBytes(isoBytes []byte) string {

	// ISO-8859-1을 EUC-KR 로 디코딩
	decoder := korean.EUCKR.NewDecoder()
	decodedData, err := decoder.Bytes(isoBytes)
	if err != nil {
		fmt.Println("디코딩 오류:", err)
		return ""
	}

	return string(decodedData)
}

// SplitByPipeline 주어진 문자열을 pipeline 을 기준으로 자른 배열을 반환하는 함수
func SplitByPipeline(origin string) []string {
	return strings.Split(origin, "|")
}

// ToMongoInsertType 주어진 이중 string 배열을 각각 json 형태로 만들어 반환하는 함수
func ToMongoInsertType(textArr [][]string) []interface{} {
	var buildings []interface{}

	for _, building := range textArr {
		birthday := ""
		if len(building[60]) != 0 {
			birthday = building[60]
		}

		grossFloorArea := float64(-1)
		if len(building[28]) != 0 {
			grossFloorArea, _ = strconv.ParseFloat(building[28], 64)
		}

		underground := -1
		if len(building[44]) != 0 {
			underground, _ = strconv.Atoi(building[44])
		}

		ground := -1
		if len(building[43]) != 0 {
			ground, _ = strconv.Atoi(building[43])
		}

		passenger := -1
		if len(building[45]) != 0 {
			passenger, _ = strconv.Atoi(building[45])
		}

		emergency := -1
		if len(building[46]) != 0 {
			emergency, _ = strconv.Atoi(building[46])
		}

		slots := 0
		if len(building[50]) != 0 {
			indrAutoUtcnt, _ := strconv.Atoi(building[50])
			slots += indrAutoUtcnt
		}
		if len(building[52]) != 0 {
			oudrAutoUtcnt, _ := strconv.Atoi(building[52])
			slots += oudrAutoUtcnt
		}
		if len(building[54]) != 0 {
			indrMechUtcnt, _ := strconv.Atoi(building[54])
			slots += indrMechUtcnt
		}
		if len(building[56]) != 0 {
			oudrMechUtcnt, _ := strconv.Atoi(building[56])
			slots += oudrMechUtcnt
		}

		// TODO: Comment 논의 필요

		street := ""
		if len(building[6]) != 0 {
			street = building[6]
		}

		name := ""
		if len(building[7]) != 0 {
			name = building[7]
		}

		buildings = append(buildings, Building{
			ID:             building[0],
			Address:        building[5],
			Birthday:       birthday,
			GrossFloorArea: grossFloorArea,
			Underground:    underground,
			Ground:         ground,
			Passenger:      passenger,
			Emergency:      emergency,
			Slots:          slots,
			Comment:        "",
			Street:         street,
			Name:           name,
		})
	}

	return buildings
}
