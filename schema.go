package outbox

import (
	"encoding/binary"
	"fmt"
)

func EncodeMessage(schemaID int, data []byte) []byte {
	buf := make([]byte, 5+len(data))
	buf[0] = 0x00
	binary.BigEndian.PutUint32(buf[1:5], uint32(schemaID))
	copy(buf[5:], data)
	return buf
}

func DecodeMessage(data []byte) (int, []byte, error) {
	if len(data) < 5 {
		return 0, nil, fmt.Errorf("message too short")
	}
	if data[0] != 0x00 {
		return 0, nil, fmt.Errorf("invalid magic byte")
	}
	schemaID := int(binary.BigEndian.Uint32(data[1:5]))
	return schemaID, data[5:], nil
}
