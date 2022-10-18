package message

// MessageOption options for Message
type MessageOption func(m *Message)

// WithHeaders is an option to set additional headers onto the Message
func WithHeaders(headers Metadata) MessageOption {
	return func(m *Message) {
		for key, value := range headers {
			m.Metadata.Set(key, value)
		}
	}
}
