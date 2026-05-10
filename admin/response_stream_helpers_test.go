package admin

import "testing"

func TestExtractResponseOutputText(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{
			name: "delta event",
			data: `{"type":"response.output_text.delta","delta":"ok"}`,
			want: "ok",
		},
		{
			name: "done event",
			data: `{"type":"response.output_text.done","text":"done"}`,
			want: "done",
		},
		{
			name: "content part done event",
			data: `{"type":"response.content_part.done","part":{"type":"output_text","text":"part"}}`,
			want: "part",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractResponseOutputText([]byte(tt.data)); got != tt.want {
				t.Fatalf("extractResponseOutputText() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractResponseFailureMessage(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{
			name: "top level error",
			data: `{"type":"response.failed","error":{"message":"top level"}}`,
			want: "top level",
		},
		{
			name: "response error",
			data: `{"type":"response.failed","response":{"error":{"message":"nested"}}}`,
			want: "nested",
		},
		{
			name: "status details fallback",
			data: `{"type":"response.failed","response":{"status_details":{"error":{"message":"details"}}}}`,
			want: "details",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractResponseFailureMessage([]byte(tt.data)); got != tt.want {
				t.Fatalf("extractResponseFailureMessage() = %q, want %q", got, tt.want)
			}
		})
	}
}
