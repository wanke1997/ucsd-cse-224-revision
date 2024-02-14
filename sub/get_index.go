package sub

import (
	"fmt"
	"math"
)

func getIndex(key []byte, amount int) int {
	numIdxs := int(math.Log2(float64(amount)))
	res := 0
	for i := 0; i < numIdxs; i += 1 {
		currByteLoc := i / 8
		currByte := key[currByteLoc]
		mask := (uint8(1) << (8 - i%8))
		if (mask & currByte) != 0 {
			res = res*2 + 1
		} else {
			res = res * 2
		}
	}
	return res
}

func main() {
	key := make([]byte, 8)
	amount := 4
	key[0] = 255

	res := getIndex(key, amount)
	fmt.Println("index =", res)
}
