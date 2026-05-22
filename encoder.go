package outbox

type Encoder interface {
	Encode(key string, protoBytes []byte) (encodedKey, encodedValue []byte, err error)
}

type BinaryEncoder struct{}

func NewBinaryEncoder() *BinaryEncoder {
	return &BinaryEncoder{}
}

func (e *BinaryEncoder) Encode(key string, protoBytes []byte) ([]byte, []byte, error) {
	return []byte(key), protoBytes, nil
}

type SchemaRegistryEncoder struct {
	schemaIDKey   int
	schemaIDValue int
}

func NewSchemaRegistryEncoder(schemaIDKey, schemaIDValue int) *SchemaRegistryEncoder {
	return &SchemaRegistryEncoder{
		schemaIDKey:   schemaIDKey,
		schemaIDValue: schemaIDValue,
	}
}

func (e *SchemaRegistryEncoder) Encode(key string, protoBytes []byte) ([]byte, []byte, error) {
	encodedKey := EncodeMessage(e.schemaIDKey, []byte(key))
	encodedValue := EncodeMessage(e.schemaIDValue, protoBytes)
	return encodedKey, encodedValue, nil
}
