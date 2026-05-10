package admin

import "github.com/tidwall/gjson"

func extractResponseOutputText(data []byte) string {
	switch gjson.GetBytes(data, "type").String() {
	case "response.output_text.delta":
		return gjson.GetBytes(data, "delta").String()
	case "response.output_text.done":
		return gjson.GetBytes(data, "text").String()
	case "response.content_part.done":
		if gjson.GetBytes(data, "part.type").String() == "output_text" {
			return gjson.GetBytes(data, "part.text").String()
		}
	}
	return ""
}

func extractResponseFailureMessage(data []byte) string {
	for _, path := range []string{
		"error.message",
		"response.error.message",
		"response.status_details.error.message",
	} {
		if message := gjson.GetBytes(data, path).String(); message != "" {
			return message
		}
	}
	return "上游返回 response.failed"
}
